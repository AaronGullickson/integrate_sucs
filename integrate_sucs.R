library(tidyverse)
library(readxl)
library(googlesheets4)
library(xml2)
library(megamekR)
library(here)

YAML_PATH <- "~/personal/Battletech/mek_project/mekhq/MekHQ/data/universe/planetary_systems/canon_systems"


# Read in data ------------------------------------------------------------

# read in xml faction data directly from GitHub
mhq_factions <- read_xml("https://raw.githubusercontent.com/MegaMek/mekhq/refs/heads/master/MekHQ/data/universe/factions.xml") |>
  xml_children() |>
  map_chr(function(node) {
    xml_text(xml_find_first(node, "shortname"))
  })


# read in faction crosswalk
gs4_deauth()
faction_crosswalk <- read_sheet("117Mmhf7TtyumwCzB9bGKq05-SeTFKboZ7Ef2ZbyOEPk",
                                sheet = "translator") |>
  rename(sucs_faction = their_code, faction = our_code) |>
  select(sucs_faction, faction)


# read in actual SUCS data
sucs_data <- read_xlsx(here("input",
                            "Sarna Unified Cartography Kit (Official).xlsx"),
                       skip = 1,
                       sheet = "Systems") |>
  select(-systemName, -alternateName, -x, -y, -size, -sarnaLink, -`distance (LY)`) |>
  rename(sucsId = systemId)

# fix names (this could be better)
colnames(sucs_data) <- make.names(colnames(sucs_data))

# pivoting longer should make it easier to deal with
sucs_data <- sucs_data |>
  pivot_longer(cols = starts_with("X"),
               names_prefix = "X",
               names_to = "time_point",
               values_to = "faction") |>
  mutate(
    faction = case_when(
      str_detect(faction, "^D\\(") ~ str_extract(faction, "(?<=\\()[^()]+(?=\\))"),
      TRUE ~ str_split_i(faction, ",", 1)
    ),
    faction = str_trim(faction), # remove any leading/trailing whitespace
    faction = str_remove(faction, "\\s*\\([^\\)]+\\)"), # remove any parentheticals
    time_point = str_remove(time_point, ".0$")
  )

# remove missing and U cases
sucs_data <- sucs_data |>
  filter(!is.na(faction) & !(faction %in% c("U","s","v")))

# before we crosswalk, we need to fix FedCom. They start doing FedCom dates as
# early as

# crosswalk their faction codes to our faction codes
# joining won't work here because of the cases where I have multiple factions
# I need to loop through all cases in my crosswalk and apply a str_replace
# TODO: deal with clan protectorate
# TODO: I feel like this could be put in a function and then called via pmap
# or something similar and would be faster and easier to extend
for(i in 1:nrow(faction_crosswalk)) {
  old_faction <- faction_crosswalk$sucs_faction[i]
  new_faction <- faction_crosswalk$faction[i]
  # first look for the whole string
  sucs_data$faction <- str_replace(sucs_data$faction,
                                   paste("^", old_faction, "$", sep = ""),
                                   new_faction)
  # now look for the word followed by comma
  sucs_data$faction <- str_replace(sucs_data$faction,
                                   paste("^", old_faction, ",", sep = ""),
                                   paste(new_faction, ",", sep = ""))
  # now look for the word at end with comma before
  sucs_data$faction <- str_replace(sucs_data$faction,
                                   paste(",", old_faction, "$", sep = ""),
                                   paste(",", new_faction, sep = ""))
  # now look for the word between commas
  sucs_data$faction <- str_replace(sucs_data$faction,
                                   paste(",", old_faction, ",", sep = ""),
                                   paste(",", new_faction, ",", sep = ""))
}

current_factions <- sucs_data |>
  filter(!str_detect(faction, ",")) |>
  pull(faction) |>
  unique() |>
  sort()

current_factions[!(current_factions %in% mhq_factions)]
sum(is.na(sucs_data$faction))

# TODO: for now remove missing, but should fix
sucs_data <- sucs_data |>
  filter(!is.na(faction))

# group_split by sucsId and then map to remove all cases after the first that
# are the same as the previous case, so that we only identify faction changes.
# Then bind_rows
sucs_data <- sucs_data |>
  group_by(sucsId) |>
  group_split() |>
  map(function(x) {
    x |>
      # we use a default for lag that guarantees a change on the first value
      mutate(change = faction != dplyr::lag(faction, default = "ZZZZ")) |>
      filter(change) |>
      select(-change)
  }) |>
  bind_rows()

# convert into the format of date, faction, source_faction
sucs_data <- sucs_data |>
  mutate(year = as.integer(str_remove(time_point, "\\D")),
         month = case_when(
           time_point == "3050a" ~ 4,
           time_point == "3050b" ~ 6,
           time_point == "3050c" ~ 7,
           time_point == "3059a" ~ 6,
           time_point == "3059b" ~ 7,
           time_point == "3059c" ~ 8,
           time_point == "3059d" ~ 12,
           TRUE ~ 12
         ),
         day = 1,
         date = as_date(paste(year, month, day, sep="-")),
         # move to last day of that month
         date = ceiling_date(date, unit = "month") - days(1),
         source_faction = "sucs") |>
  select(sucsId, date, faction, source_faction) |>
  #remove data from 2571 and earlier because sketchy
  filter(year(date) > 2571)

# load in planet data in a map, get the subset of sucs_data that matches
# sucsId. Remove existing sucs codes from planetary event data and then
# join in the new sucs codes. Save back to planetary events and write out file

# TODO: ideally we read in from GitHub for most current
planet_files <- list.files(YAML_PATH, pattern = "*.yml")

faction_events <- map(planet_files, function(planet_file) {
  planet_data <- read_planetary_data(here(YAML_PATH, planet_file))

  planet_faction <- planet_data$planetary_events[[planet_data$system$primarySlot]] |>
    filter(!is.na(faction)) |>
    select(date, faction, source_faction) |>
    # remove any existing sucs data
    filter(is.na(source_faction) | source_faction != "sucs")

  planet_sucs <- sucs_data |>
    filter(sucsId == planet_data$system$sucsId) |>
    filter(date >= min(planet_faction$date)) |>
    select(-sucsId)

  planet_faction <- planet_faction |>
    bind_rows(planet_sucs) |>
    arrange(date)

}) |> bind_rows()
