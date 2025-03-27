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
# can't do this by join because of multiple faction issue, but I can
# use str_replace_all with a named vector to do them all in one good
faction_replacement <- faction_crosswalk$faction
# if we set the names to equal the regex to match on we can make this work
# we have to use look arounds to get boundaries that could be commas or
# start/end of character string
names(faction_replacement) <- paste("(?<=^|,)",
                                    faction_crosswalk$sucs_faction,
                                    "(?=,|$)",
                                    sep = "")
sucs_data$faction <- str_replace_all(sucs_data$faction, faction_replacement)

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



# Clean up the SUCS data for our format -----------------------------------

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

# Merge and write out new data --------------------------------------------

update_from_sucs <- function(planet_file) {

  # read in the data
  planet_data <- read_planetary_data(here(YAML_PATH, planet_file))

  # get corresponding SUCS data
  planet_sucs <- sucs_data |>
    filter(sucsId == planet_data$system$sucsId) |>
    select(-sucsId)

  # we assume faction changes are the same for all inhabited planets in the
  # system
  planet_data$planetary_events <- planet_data$planetary_events |>
    map(function(planet_events) {
      if(is.null(planet_events) | !("faction" %in% colnames(planet_events))) {
        return(NULL)
      }
      # get just the faction events
      planet_faction_events <- planet_events |>
        filter(!is.na(faction)) |>
        select(date, faction, source_faction) |>
        # remove any existing sucs data
        filter(is.na(source_faction) | source_faction != "sucs") |>
        arrange(date)

      # check for abandon date
      n <- nrow(planet_faction_events)
      founding_date <- min(planet_events$date)
      abandon_date <- ifelse(planet_faction_events$faction[n] == "ABN",
                             planet_faction_events$date[n],
                             Inf)


      # bind in the sucs data
      planet_faction_events <- planet_sucs |>
        # remove any dates before the founding or after final abandonment
        # TODO: we should really make a note if this happens to primary
        # as it suggests data discrepancies
        filter(date > founding_date & date < abandon_date) |>
        bind_rows(planet_faction_events) |>
        arrange(date)

      # now we can remove existing faction data from planet_events and merge
      # in the updated version
      planet_events <- planet_events |>
        select(-faction, -source_faction) |>
        filter(!if_all(!date, is.na))

      planet_events <- planet_faction_events |>
        full_join(planet_events, by = "date") |>
        arrange(date)

      return(planet_events)
    })

  write_planetary_data(planet_data, here(YAML_PATH, planet_file))

  return(TRUE)
}

# TODO: ideally we read in from GitHub for most current
planet_files <- list.files(YAML_PATH, pattern = "*.yml")
results <- map_lgl(planet_files, possibly(update_from_sucs, FALSE))

sum(!results)
