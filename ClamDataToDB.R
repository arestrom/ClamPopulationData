#=======================================================================================
# Example script to load clam population data to ps_shellfish database. 
#
# ClamPopulationData repo holds only data needed for 2022. Intended for transfer to Jack D. 
#
# Steps:
#  1. 
#
#  ToDo:
#  1.
# 
#  Notes: 
#  1. The gdb files in 2022 GPS and Field Notes do not download using sf in R
#     But the gpx files open correctly. The gdb files do open in QGIS and match
#     the gpx files exactly for WPennCove 2022. 
#  2. Doug confirmed any and all edits to pop data will be in the BeachEdits folder
#     (ie, 2022.240150.00Bch.xlsx) for location and basic transect data. So use the 
#     BeachEdits data as baseline. Compare beach edits to gpx and gdb files.
#  3. Doug confirmed that he would never go back to find missing samples. Very rare
#     occurrance was Karla way back when. It is ok to interpolate positions and times
#     for cases where sample location and times are missing.
#  4. Doug confirmed that tallyclam and wet lab data to not overlap. For cases where
#     all sample numbers were brought back to wet lab, there would be no tally clam 
#     data for the beach (ie. Point Whitney, 2022). In 2020 there were basically no
#     wet lab data due to COVID. 
#  5. Doug confirmed that for wet lab data the correct data to use are in the Safe Copies
#     folder. 
#  6. Can calculate lead distance with code from:
#     https://stackoverflow.com/questions/49853696/distances-of-points-between-rows-with-sf
#  7. There were incomplete data in the Access DBs for TallyClam in the 2022 Clam Tally Data
#     folder. The comprehensive Access DB at: Clam&OysterPopSurveyData looks to be complete.
#
# AS 2023-06-29
#=======================================================================================

# Clear workspace
rm(list = ls(all.names = TRUE))

# Load libraries
library(RODBC)
library(DBI)
library(RPostgres)
library(dplyr)
library(uuid)
library(tidyr)
library(sf)
library(stringi)
library(lubridate)
library(glue)
library(openxlsx)
library(fs)

# Set options
options(digits=15)

# Keep connections pane from opening
options("connectionObserver" = NULL)

#=====================================================================================
# Function to get user for database
pg_user <- function(user_label) {
  Sys.getenv(user_label)
}

# Function to get pw for database
pg_pw <- function(pwd_label) {
  Sys.getenv(pwd_label)
}

# Function to get pw for database
pg_host <- function(host_label) {
  Sys.getenv(host_label)
}

# Function to connect to postgres
pg_con_local = function(dbname, port = '5432') {
  con <- dbConnect(
    RPostgres::Postgres(),
    host = pg_host("pg_host_local"),
    dbname = dbname,
    user = pg_user("pg_user"),
    password = pg_pw("pg_pwd_local"),
    port = port)
  con
}

# Generate a vector of Version 4 UUIDs (RFC 4122)
get_uuid = function(n = 1L) {
  if (!typeof(n) %in% c("double", "integer") ) {
    stop("n must be an integer or double")
  }
  uuid::UUIDgenerate(use.time = FALSE, n = n)
}

#============================================================================
# Import all data for survey and encounters
#============================================================================

# Define current_year
current_year = 2022

# Define path to GPS data
gps_path = "data\\2022\\2022 GPS and Field Notes\\CLAM\\GPS Downloaded Maps\\"
beach_edits_path = "data\\2022\\2022 GPS and Field Notes\\CLAM\\Beach Edits\\"
beach_number_path = "data\\2022\\2022 Clam\\2022 Clam Tally Data\\2022.09.14TallyClam.mdb"
tally_path = "data\\Clam&OysterPopSurveyData\\CLAM\\ClamTally2002-2022.accdb"
wet_lab_path = "data\\Clam&OysterPopSurveyData\\CLAM\\ClamPopLengthWeight.accdb"

# Check extensions that occur in the pictures folder
gps_files = dir_ls(gps_path, recurse = TRUE)
unique(tools::file_ext(gps_files))

# Make vector of all the gpx files
gpx_paths = subset(gps_files, tools::file_ext(gps_files) %in% c("gpx"))

# Inspect
# head(gpx_paths, 20)
(gpx_files = basename(gpx_paths))

# Make vector of all beach edits xlsx files
beach_edit_files = dir_ls(beach_edits_path, recurse = TRUE)
unique(tools::file_ext(beach_edit_files))

# Make vector of all xlsx files in the folder
xlsx_paths = subset(beach_edit_files, tools::file_ext(beach_edit_files) %in% c("xlsx"))

# Inspect
(xlsx_files = basename(xlsx_paths))

# Get the bidn names
con = odbcConnectAccess2007(beach_number_path)
beach_id = RODBC::sqlQuery(con, as.is = TRUE, "SELECT * FROM BIDN")
close(con) 

# Get the clam tally data. The only complete source in 2022
con = odbcConnectAccess2007(tally_path)
legals = RODBC::sqlQuery(con, as.is = TRUE, "SELECT * FROM Legals")
close(con) 

# Get the wet lab data. Assume it's complete. Use WetLabData SafeCopies xlsx files if not. 
con = odbcConnectAccess2007(wet_lab_path)
wet_lab = RODBC::sqlQuery(con, as.is = TRUE, "SELECT * FROM LengthWeight")
close(con) 

# Organize the legals data
legals = legals |> 
  mutate(Date = substr(Date, 1, 10)) |> 
  select(bidn = BIDN, date_part = Date, sample_number = `Sample Number`,
         manila = Manila, native = Native, butter = Butter, cockle = Cockle,
         eastern = Eastern, horse = Horse, varnish = Varnish)

# Organize the wet_lab data
wet_lab = wet_lab |> 
  mutate(BIDN = as.integer(BIDN)) |> 
  select(BIDN, year = Year, sample_number = Sample_Number, species = Species,
         count = Count, length = Length, weight = Weight, flag = Flag)

# Get location IDs for the current year beaches
con = pg_con_local("ps_shellfish")
qry = glue("SELECT loc.location_id, lb.boundary_code, lb.boundary_name, ",
           "lb.active_datetime ",
           "FROM location as loc ",
           "left join location_type_lut as lt ",
           "on loc.location_type_id = lt.location_type_id ",
           "left join location_boundary as lb on ",
           "loc.location_id = lb.location_id ",
           "where lt.location_type_description = 'Beach polygon, Intertidal management' ",
           "and date_part('year', active_datetime) = {current_year} ",
           "order by active_datetime desc")
beach_info = DBI::dbGetQuery(con, qry)
DBI::dbDisconnect(con)

# Check for duplicate BIDNs
if ( any(duplicated(beach_info$boundary_code)) ) {
  cat("\nAt least one duplicated BIDN. Investigate!\n\n") 
} else {
  cat("\nNo duplicated BIDNs. Ok to proceed.\n\n")
}

# Check for duplicate location IDs
if ( any(duplicated(beach_info$location_id)) ) {
  cat("\nAt least one duplicated location ID. Investigate!\n\n") 
} else {
  cat("\nNo duplicated location IDs. Ok to proceed.\n\n")
}

# Check for duplicate beach names
if ( any(duplicated(beach_info$boundary_name)) ) {
  cat("\nAt least one duplicated beach name. Investigate!\n\n") 
} else {
  cat("\nNo duplicated beach names. Ok to proceed.\n\n")
}

#==============================================================================================
# Initial beach, can increment for additional beaches. But check carefully.
#==============================================================================================

# Define the increment
i = 1

# Define the bidn
beach_number = substr(gpx_files[i], 6, 14)

# Get data from gpx and xlsx to compare.
dat_gpx = st_read(gpx_paths[i], layer = "waypoints") |> 
  mutate(latitude = as.numeric(st_coordinates(geometry)[,2])) |> 
  mutate(longitude = as.numeric(st_coordinates(geometry)[,1])) |> 
  mutate(BIDN = beach_number) |> 
  mutate(waypoint = as.integer(name)) |> 
  left_join(beach_id, by = "BIDN") |> 
  select(waypoint, bidn = BIDN, beach_name = Name, time, latitude, longitude, elevation = ele)

# Check crs
st_crs(dat_gpx)$epsg

# # Get data from gdb to compare. Will not open. Need to use QGIS
# wpenncoveb = terra::vect(glue(gps_path, "2022.240150.00.gdb"), layer = "waypoints")
# wpenncoveb = sf::st_read(glue(gps_path, "2022.240150.00.gdb"), layer = "waypoints")

# Read xlsx
dat_xlsx = read.xlsx(xlsx_paths[i], detectDates = TRUE) |> 
  mutate(time = openxlsx::convertToDateTime(Description)) |> 
  mutate(BIDN = beach_number) |> 
  mutate(Waypoint = as.integer(Waypoint)) |> 
  mutate(geometry = mapply(function(x,y) st_point(c(x,y)), 
                           Longitude, Latitude,SIMPLIFY = FALSE)) |> 
  mutate(geometry = st_as_sfc(geometry, crs = 4326)) |> 
  filter(!X16 %in% c("sqft", "acres")) |>
  left_join(beach_id, by = "BIDN") |> 
  mutate(sample_number = as.integer(SAMPNO)) |> 
  select(waypoint = Waypoint, bidn = BIDN, beach_name = Name, time,
         latitude = Latitude, longitude = Longitude, surveyor = SURVEYOR,
         pace = PACE, waypoint_type = TYPE, sample_number, 
         transect = TRANSECT, transpace = TRANSPAC,
         shorepace = SHOREPAC, geometry)
  
# Join to compare datetimes and coordinates
nrow(dat_gpx); nrow(dat_xlsx)
nrow(dat_gpx) == nrow(dat_xlsx)
chk_dat = dat_gpx |> 
  select(waypoint, time_gpx = time, lat = latitude, lon = longitude) |> 
  left_join(dat_xlsx, by = "waypoint") |> 
  mutate(time_chk = time_gpx - time) |>
  mutate(dist_chk = st_distance(geometry.x, geometry.y, by_element = TRUE)) |> 
  mutate(dist_m = round(dist_chk, digits = 4)) |> 
  st_drop_geometry() |> 
  select(waypoint, time_gpx, time, time_chk, dist_m)

# # Add some error to test code below
# chk_dat$time_chk[1] = chk_dat$time_chk[1] + 2
# chk_dat$dist_m[2] = chk_dat$dist_m[2] + units::set_units(2, m)

# Pull out rows that differ in time or location
chk_dat = chk_dat |> 
  filter(is.na(time_chk) | 
           time_chk > as.difftime("00:00:00") | 
           is.na(dist_m) | 
           dist_m > units::set_units(1, m))

# Result: Use xlsx. Two rows deleted. Sample numbers are consecutive in xlsx across deletions
#         But add elevation from gpx file
gpx_elev = dat_gpx |> 
  st_drop_geometry() |> 
  select(waypoint, elevation)

dat_xlsx = dat_xlsx |> 
  left_join(gpx_elev, by = "waypoint") |> 
  select(waypoint, bidn, beach_name, time, latitude, longitude, elevation, 
         surveyor, pace, waypoint_type,sample_number, transect, transpace, 
         shorepace, geometry)

# Filter out tally data for current beach and year
tally_dat = legals |> 
  filter(bidn == as.numeric(beach_number) & substr(date_part, 1, 4) == "2022") |> 
  select(beach_number = bidn, date_part, sample_number, manila, native, butter,
         cockle, eastern, horse, varnish)

# Combine tally data with base survey data
dat = dat_xlsx |> 
  mutate(sample_number = as.numeric(sample_number)) |> 
  left_join(tally_dat, by = "sample_number")

# Sanity check
if ( nrow(dat) != nrow(dat_xlsx) ) {
  cat("\nIncorrect number of rows in dat. Investigate!\n\n")
} else {
  cat("\nNumber of rows correct. Ok to proceed\n\n")
}
  
#==========================================================
# Add summary info from wet lab. Verify all samples present
#==========================================================

# Pull out current_year and beach wet_lab data
wet_dat = wet_lab |> 
  filter(BIDN == as.integer(beach_number) & year == as.numeric(current_year)) |> 
  select(BIDN, year_part = year, sample_number, species, count, length, weight,
         flag)

# Inspect BIDN and year entries
unique(wet_dat$BIDN)
unique(wet_dat$year)

# Create a summary dataset with sample_number and counts for matching with dat
wet_sums = wet_dat |> 
  group_by(sample_number) |> 
  summarise(wet_count = sum(count, na.rm = TRUE)) |> 
  ungroup() |> 
  select(sample_number, wet_count)

# Check for duplicated sample_numbers
any(duplicated(wet_sums$sample_number))

# Check if there are any sample numbers in wet_sums that are not in dat
# Should be none
wet_sums$sample_number[!wet_sums$sample_number %in% dat$sample_number]
# And the reverse...should be several
# dat$sample_number[!dat$sample_number %in% wet_sums$sample_number]

# Add wet_sums to dat
dat = dat |> 
  left_join(wet_sums, by = "sample_number") 

# Check that all sample numbers have a count associated
cols = c("manila", "native", "butter", "cockle", "eastern", "horse", "varnish")
chk_dat = dat |> 
  st_drop_geometry() |> 
  mutate(tally_count = rowSums(across(all_of(cols)))) |> 
  select(bidn, beach_number, beach_name, time, sample_number, wet_count, tally_count)

# Filter to see if any cases with sample_number have neither a wet count or tally count
chk_dat2 = chk_dat |> 
  filter(!is.na(sample_number) & is.na(wet_count) & is.na(tally_count))

# Sanity check
if ( nrow(chk_dat2) > 0L ) {
  cat("\nSome sample_numbers unnaccounted for. Investigate!\n\n")
} else {
  cat("\nAll sample numbers accounted for. Ok to proceed\n\n")
}

#==========================================================
# Generate survey level data
#==========================================================

# Check geometry in dat
st_crs(dat$geometry)$epsg

# Pull out survey level data
srv = dat |> 
  st_drop_geometry() |> 
  select(bidn, time, surveyor) |> 
  distinct() |> 
  mutate(survey_datetime = with_tz(min(time), "UTC")) |> 
  mutate(start_datetime = with_tz(min(time), "UTC")) |> 
  mutate(end_datetime = with_tz(max(time), "UTC"))

# Start trimming and adding as needed
survey = srv |> 
  select(bidn, survey_datetime, start_datetime, end_datetime, surveyor) |> 
  distinct()

# There must be only one row...Verify!
if ( nrow(survey) > 1L ) {
  cat("\nThere must be only one row. Investigate!\n\n")
} else {
  cat("\nOnly one row. Ok to proceed!\n\n")
}

# Add boundary ID 
beach_info = beach_info |> 
  mutate(bidn = paste0(boundary_code, ".00")) |> 
  select(location_id, boundary_code, bidn, boundary_name, active_datetime)

# Add info
survey = survey |> 
  left_join(beach_info, by = "bidn") |> 
  mutate(survey_id = get_uuid(1L)) |> 
  mutate(survey_type_id = "4bfd73c6-79dd-4428-a2b7-d8f1aa2326a4") |>               # Clam Population Survey
  mutate(organizational_unit_id = "f1de1d54-d750-449f-a700-dc33ebec04c6") |>       # Intertidal Mngmnt Unit
  mutate(area_surveyed_id = "4e072477-e5f1-4a03-a14c-18e4aeade76e") |>             # Not applicable. Have coords.
  mutate(data_review_status_id = "59cbf9d7-9015-49b6-874d-ea37ef663c56") |>        # Reviewed
  mutate(survey_completion_status_id = "d192b32e-0e4f-4719-9c9c-dec6593b1977") |>  # Completed survey
  mutate(comment_text =                                                            # From PopWork.xlsx
           "Four first time clam samplers (GWH, Stilliguamish Tribe, WCC)") |> 
  mutate(created_datetime = with_tz(Sys.time(), "UTC")) |> 
  mutate(created_by = "stromas") |>                                                # Don't use in future !!!
  #mutate(created_by = Sys.getenv("USERNAME")) |>                                  # Use this line instead !!!
  mutate(modified_datetime = with_tz(as.POSIXct(NA), "UTC")) |> 
  mutate(modified_by = NA_character_) |> 
  select(survey_id, survey_type_id, organizational_unit_id, location_id,
         area_surveyed_id, data_review_status_id, survey_completion_status_id,
         survey_datetime, start_datetime, end_datetime, comment_text, 
         created_datetime, created_by, modified_datetime, modified_by)
  
# Generate data for person table (ONLY IF NOT ALREADY PRESENT IN DATABASE !!!!!!!!!!!!!!!!)
person = tibble::tibble(
  person_id = get_uuid(2L),
  first_name = c("Camille", "Doug"),
  last_name = c("Speck", "Rogers"),
  active_indicator = rep(1L, 2),
  created_datetime = rep(with_tz(Sys.time(), "UTC"), 2),
  created_by = rep("stromas", 2),                              # Do not use in future !!!!!
  #created_by = rep(Sys.getenv("USERNAME"), 2),                # Use this line instead !!!!
  modified_datetime = rep(with_tz(as.POSIXct(NA), "UTC"), 2),
  modified_by = rep(NA_character_, 2))

# Generate data for survey_person table...PULL FROM DB FOR SUBSEQUENT CASES !!!!!!!!!!!!!
survey_person = tibble::tibble(
  survey_person_id = get_uuid(1L),
  survey_id = survey$survey_id,
  person_id = person$person_id[1])        # Edit here for use in future !!!!!!

# Any duplicated waypoints?
any(duplicated(dat$waypoint))

# Generate survey_event data. Make sure waypoint is not duplicated. It is used for join later
srv_event = dat |> 
  mutate(survey_id = survey$survey_id) |> 
  mutate(event_number = as.integer(waypoint)) |> 
  mutate(event_datetime = with_tz(time, "UTC")) |> 
  select(survey_id, event_number, event_datetime) |> 
  distinct()

# Add needed columns
srv_event = srv_event |> 
  mutate(survey_event_id = get_uuid(nrow(srv_event))) |> 
  mutate(event_location_id = get_uuid(nrow(srv_event))) |> 
  mutate(harvester_type_id = "df751c90-3f80-4a3c-b67c-c496dd2201b9") |>       # Not applicable
  mutate(harvest_method_id = "68cb2cb1-77df-49f3-872b-5e3ba3299e14") |>       # Not applicable
  mutate(harvest_gear_type_id = "5d1e6be6-cd85-498c-b2fa-43b370d951b4") |>    # Not applicable
  mutate(harvest_depth_range_id = "b27281f7-9387-4a51-b91f-95748676f918") |>  # Not applicable
  mutate(harvester_count = NA_integer_) |> 
  mutate(harvest_gear_count = NA_integer_) |> 
  mutate(harvester_zip_code = NA_character_) |> 
  mutate(comment_text = NA_character_) |> 
  mutate(created_datetime = with_tz(Sys.time(), "UTC")) |> 
  mutate(created_by = "stromas") |>                                                # Don't use in future !!!
  #mutate(created_by = Sys.getenv("USERNAME")) |>                                  # Use this line instead !!!
  mutate(modified_datetime = with_tz(as.POSIXct(NA), "UTC")) |> 
  mutate(modified_by = NA_character_) |> 
  st_drop_geometry() |> 
  select(survey_event_id, survey_id, event_location_id, harvester_type_id,
         harvest_method_id, harvest_gear_type_id, harvest_depth_range_id,
         event_number, event_datetime, harvester_count, harvest_gear_count,
         harvester_zip_code, comment_text, created_datetime, created_by, 
         modified_datetime, modified_by)

# Get dataset so survey_event_id can be joined to dat via waypoint
sev_ids = srv_event |> 
  select(survey_event_id, location_id = event_location_id, waypoint = event_number)

# Check the categories of waypoint_type
unique(dat$waypoint_type)
any(is.na(dat$waypoint_type))

# Generate location and location_coordinates data for each waypoint (event_number)
surv_loc = dat |> 
  left_join(sev_ids, by = "waypoint") |> 
  mutate(location_type_id = case_when(
    waypoint_type == "Bndy-btm" ~ "5bc29f85-ee83-4ddb-886d-2ffeccb776ca",
    waypoint_type == "Bndy-top" ~ "bbdf1e05-3441-458a-9ba8-b9df97c5438e",
    waypoint_type == "Top" ~ "704cd000-e8fc-4a30-a030-7b404e4d6904",
    waypoint_type == "Bottom" ~ "f05c7866-c630-4309-8a32-b9d599a8b505",
    waypoint_type == "Sample" ~ "d96f1f74-699c-496b-a090-565b489a4738",
    is.na(waypoint_type) ~ NA_character_)) |>          
  mutate(location_code = as.character(sample_number)) |> 
  mutate(location_name = NA_character_) |> 
  mutate(location_description = NA_character_) |> 
  mutate(comment_text = NA_character_) |> 
  select(location_id, location_type_id, location_code, location_name,
         location_description, comment_text, geometry)

# Pull out just location table data
location = surv_loc |> 
  st_drop_geometry() |>
  mutate(created_datetime = with_tz(Sys.time(), "UTC")) |> 
  mutate(created_by = "stromas") |>                                                # Don't use in future !!!
  #mutate(created_by = Sys.getenv("USERNAME")) |>                                  # Use this line instead !!!
  mutate(modified_datetime = with_tz(as.POSIXct(NA), "UTC")) |> 
  mutate(modified_by = NA_character_) |> 
  select(location_id, location_type_id, location_code, location_name,
         location_description, comment_text, created_datetime, 
         created_by, modified_datetime, modified_by)
  
# Pull out location_coordinates data. Check epsg
location_coordinates = surv_loc |> 
  select(location_id, geog = geometry)
  
# Check geography in location_coordinates
location_coordinates = st_as_sf(location_coordinates)
st_crs(location_coordinates)$epsg

# Add missing columns. 
# No need for gid. Will use code from "copy_current_shellfish_to_ps_shellfish.R"
# This uses INSERT INTO syntax copying from a temp table in same database
location_coordinates = location_coordinates |> 
  mutate(location_coordinates_id = get_uuid(nrow(location_coordinates))) |> 
  mutate(horizontal_accuracy = NA_real_) |> 
  mutate(created_datetime = with_tz(Sys.time(), "UTC")) |> 
  mutate(created_by = "stromas") |>                                                # Don't use in future !!!
  #mutate(created_by = Sys.getenv("USERNAME")) |>                                  # Use this line instead !!!
  mutate(modified_datetime = with_tz(as.POSIXct(NA), "UTC")) |> 
  mutate(modified_by = NA_character_) |> 
  select(location_coordinates_id, location_id, horizontal_accuracy,
         geog, created_datetime, created_by, modified_datetime,
         modified_by)

# Get species_encounter data
spec_enc = dat |> 
  st_drop_geometry() |> 
  left_join(sev_ids, by = "waypoint") |> 
  filter(!is.na(sample_number)) |> 
  select(survey_event_id, sample_number, wet_count, manila, native,
         butter, cockle, eastern, horse, varnish)

# Pull out each category of tally count species, then stack into one dataset
manila = spec_enc |> 
  select(survey_event_id, species_count = manila, sample_number) |> 
  filter(!is.na(species_count)) |> 
  mutate(species_id = "a19761a6-f1f8-486f-af1f-94e129fbfb62") |> 
  mutate(species = "manila") |> 
  select(survey_event_id, species_id, species, sample_number, species_count)

# Pull out each category of tally count species, then stack into one dataset
native = spec_enc |> 
  select(survey_event_id, species_count = native, sample_number) |> 
  filter(!is.na(species_count)) |> 
  mutate(species_id = "906931e9-b480-44c9-b798-139bbaee4630") |> 
  mutate(species = "native") |> 
  select(survey_event_id, species_id, species, sample_number, species_count)

# Pull out each category of tally count species, then stack into one dataset
butter = spec_enc |> 
  select(survey_event_id, species_count = butter, sample_number) |> 
  filter(!is.na(species_count)) |> 
  mutate(species_id = "5c92ce13-de44-44f7-b51d-c1e50fa9ec99") |> 
  mutate(species = "butter") |> 
  select(survey_event_id, species_id, species, sample_number, species_count)

# Pull out each category of tally count species, then stack into one dataset
cockle = spec_enc |> 
  select(survey_event_id, species_count = cockle, sample_number) |> 
  filter(!is.na(species_count)) |> 
  mutate(species_id = "c4f1b299-b46a-46db-b9b4-1074744b0a07") |> 
  mutate(species = "cockle") |> 
  select(survey_event_id, species_id, species, sample_number, species_count)

# Pull out each category of tally count species, then stack into one dataset
eastern = spec_enc |> 
  select(survey_event_id, species_count = eastern, sample_number) |> 
  filter(!is.na(species_count)) |> 
  mutate(species_id = "4fb72cc7-a4c3-4cf4-acde-32bea9f15a43") |> 
  mutate(species = "eastern") |> 
  select(survey_event_id, species_id, species, sample_number, species_count)

# Pull out each category of tally count species, then stack into one dataset
horse = spec_enc |> 
  select(survey_event_id, species_count = horse, sample_number) |> 
  filter(!is.na(species_count)) |> 
  mutate(species_id = "383ca6a6-aeb9-4b67-af31-4007f3ce0dc1") |> 
  mutate(species = "horse") |> 
  select(survey_event_id, species_id, species, sample_number, species_count)

# Pull out each category of tally count species, then stack into one dataset
varnish = spec_enc |> 
  select(survey_event_id, species_count = varnish, sample_number) |> 
  filter(!is.na(species_count)) |> 
  mutate(species_id = "821c6569-8631-494a-926a-560a81075b14") |> 
  mutate(species = "varnish") |> 
  select(survey_event_id, species_id, species, sample_number, species_count)

# Stack
tally_clams = rbind(manila, native, butter, cockle, eastern, horse, varnish) |> 
  arrange(sample_number) |> 
  mutate(catch_result_type_id = case_when(
    species_count > 0L ~ "eb56984c-300d-400b-9afc-fda9a123642f",      # legal, released
    species_count == 0L ~ "8891f1e8-f01b-401a-952b-76c9d589a83c")) |> # not applicable
  mutate(shell_condition_id = "040f8149-1003-45ae-8779-628033950378") # not applicable

# Verify no catch_result_type_id missing
if ( any(is.na(tally_clams$catch_result_type_id)) ) {
  cat("\nSome catch_result_type_ids missing. Investigate!\n\n")
} else {
  cat("\nAll catch_result_type_ids present. Ok to proceed\n\n")
}

# Verify no shell_condition_id missing
if ( any(is.na(tally_clams$shell_condition_id)) ) {
  cat("\nSome shell_condition_ids missing. Investigate!\n\n")
} else {
  cat("\nAll shell_condition_ids present. Ok to proceed\n\n")
}

# Notes:
#  1. We do not need pace, transpace, or shorepace since we have coordinates
#  2. We can create a transect in the route table and name each transect by the transect number, if needed
#  3. Sample numbers are being added to location table as location_code.
#  4. Each wet-lab species has a count of one and needs to be added one line at a time to species_encounter
#     since that's where we enter the species_count value and shell condition.
#  5. Enter the length and weight to the individual_species table. 
#  6. Macoma's are given lumped weights where more than one found in a sample. Should enter all lumped counts
#     to species_encounter table. There are no lengths for macomas. Weights for lumped counts of macomas should 
#     be added to the species_encounter table. There is no advantage to adding weights for single macomas to 
#     the individual_species table, so for consistency, add to the species_encounter table.
#  7. For empty lab samples, verify that these were just sample holes with no clams.
#  8. For flooded lab samples, verify that these were sample holes that could not be dug due to tide.
#  9. For species_encounter table, species_location_id will be NULL. All locations are in survey_event table.
# 10. Enter zero for both flooded and empty sample holes. In keeping with how data are recorded. Flooded 
#     status will be noted in catch_result_type_id column.
# 11. Weight Only and Length Only flag values do not need to be explicitly recorded in DB. This will be
#     implicit in that either value will not be recorded in the weight or length columns of the
#     individual_species table.
# 12. Code above assumes shell condition for tally clams is "Not applicable" and that legal size clams
#     in the tally count includes both broken and unbroken clams. Adjust code if this is not true.

# Verify the wet_lab data all have sample numbers in beach_edit data
all(unique(wet_dat$sample_number) %in% unique(spec_enc$sample_number))
unique(wet_dat$year_part)
unique(wet_dat$BIDN)
unique(wet_dat$species)
  
# Generate wet_lab data for the species_encounter table. 
# Includes data for individual_species table
lab_clams = wet_dat |> 
  left_join(spec_enc, by = "sample_number") |> 
  select(survey_event_id, species, sample_number, species_count = count,
         wet_count, length, weight, flag)

# Check the flag codes
unique(lab_clams$flag)

# LO = length only; OK = Both length and weight; WO = Weight only; 
# BL = Broken Legal; BS = Broken Sub-legal; MT = Empty; FL = Flooded;

# Check what species are in wet-lab data
unique(lab_clams$species)

# Manually check data to verify flag and species agree on empty and flooded !!!!

# Convert species names to lower case and ess to eastern. Allows match to DB LUT values
lab_clams = lab_clams |>
  mutate(species = tolower(species)) |> 
  mutate(species = if_else(species == "ess", "eastern", species)) |> 
  mutate(species = if_else(species %in% c("empty", "flooded"), 
                           "na", species)) 

# Check again
unique(lab_clams$species)

# Get species_lut values...may need to add new values to table over time and re-import
con = pg_con_local("ps_shellfish")
qry = glue("SELECT species_id, species_code ",
           "FROM species_lut")
species_ids = DBI::dbGetQuery(con, qry)
DBI::dbDisconnect(con)

# Join to get all species_ids
lab_clams = lab_clams |> 
  left_join(species_ids, by = c("species" = "species_code"))

# Verify none are missing
if ( any(is.na(lab_clams$species_id)) ) {
  cat("\nSome species_ids missing. May need to add values to LUT. Do not pass go!\n\n")
} else {
  cat("\nAll species_ids present. Ok to proceed\n\n")
}

# Check flag values
unique(lab_clams$flag)

# Pull out data with missing values to decide how to fill
chk_flag = lab_clams |> 
  filter(is.na(flag))

# IN THIS CASE ALL CAN BE FILLED WITH OK....NEED TO VERIFY EACH TIME
lab_clams = lab_clams |> 
  mutate(flag = if_else(is.na(flag), "OK", flag))

# Check flag values again
unique(lab_clams$flag)

# Add catch result type info
lab_clams = lab_clams |> 
  mutate(catch_result_type_id = case_when(
    flag == "MT" ~ "95570f30-8bbb-4bd9-8b62-7f44f67b3baf",
    flag == "FL" ~ "91d362a6-ac83-4aac-8ff2-b2038a0184dc",
    !flag %in% c("MT", "FL") ~ "31d9f2fe-7afe-495f-938c-59a33bd22d9e"))

# Check
unique(lab_clams$catch_result_type_id)

# Verify no catch_result_type_id missing
if ( any(is.na(lab_clams$catch_result_type_id)) ) {
  cat("\nSome catch_result_type_ids missing. Investigate!\n\n")
} else {
  cat("\nAll catch_result_type_ids present. Ok to proceed\n\n")
}

# Add shell condition ID
lab_clams = lab_clams |> 
  mutate(shell_condition_id = case_when(
    flag %in% c("OK", "WO", "LO") ~ "aea28523-5e72-4830-8b8a-12f064c3dab4",
    flag == "BS" ~ "19966e60-8061-4796-b211-ea53f578f662",
    flag == "BL" ~ "f407a187-bfb1-496c-9b69-457df1dbd123",
    flag %in% c("FL", "MT") ~ "040f8149-1003-45ae-8779-628033950378",
    is.na(species) ~ "040f8149-1003-45ae-8779-628033950378",
    is.na(flag) ~ "aea28523-5e72-4830-8b8a-12f064c3dab4"))

# Verify no shell_condition_id missing
if ( any(is.na(lab_clams$shell_condition_id)) ) {
  cat("\nSome shell_condition_ids missing. Investigate!\n\n")
} else {
  cat("\nAll shell_condition_ids present. Ok to proceed\n\n")
}

# Pull out tally data for species_encounter table
lab_encounter = lab_clams |> 
  mutate(species_weight_gram = if_else(species == "macoma", weight, NA_real_)) |> 
  select(survey_event_id, species_id, catch_result_type_id,
         shell_condition_id, species_count, 
         species_weight_gram, length, weight)

# Pull out wet-lab data for species_encounter table
tally_encounter = tally_clams |> 
  mutate(species_weight_gram = NA_real_) |> 
  select(survey_event_id, species_id, catch_result_type_id,
         shell_condition_id, species_count)

# Combine
spec_encounter = bind_rows(lab_encounter, tally_encounter)

# Add remaining rows
spec_encounter = spec_encounter |> 
  mutate(species_encounter_id = get_uuid(nrow(spec_encounter))) |> 
  mutate(species_location_id = NA_character_) |> 
  mutate(no_head_indicator = 0L) |> 
  mutate(comment_text = NA_character_) |> 
  mutate(created_datetime = with_tz(Sys.time(), "UTC")) |> 
  mutate(created_by = "stromas") |>                                                # Don't use in future !!!
  #mutate(created_by = Sys.getenv("USERNAME")) |>                                  # Use this line instead !!!
  mutate(modified_datetime = with_tz(as.POSIXct(NA), "UTC")) |> 
  mutate(modified_by = NA_character_) |> 
  select(species_encounter_id, survey_event_id, species_id,
         species_location_id, catch_result_type_id, 
         shell_condition_id, species_count, species_weight_gram,
         no_head_indicator, comment_text, created_datetime,
         created_by, modified_datetime, modified_by,
         length, weight)

# Pull out species_encounter
species_encounter = spec_encounter |> 
  select(species_encounter_id, survey_event_id, species_id,
         species_location_id, catch_result_type_id, 
         shell_condition_id, species_count, species_weight_gram,
         no_head_indicator, comment_text, created_datetime,
         created_by, modified_datetime, modified_by)

# Pull out individual_species
individual_species = spec_encounter |> 
  filter(species_count == 1L) |> 
  filter(!(is.na(length) & is.na(weight)))

# Add missing columns
individual_species = individual_species |> 
  mutate(individual_species_id = get_uuid(nrow(individual_species))) |> 
  mutate(sex_id = "d1c67348-3181-454b-a6a2-1b777e7914f8") |> 
  mutate(species_sample_number = NA_character_) |> 
  select(individual_species_id, species_encounter_id,
         sex_id, species_sample_number, weight_measurement_gram = weight,
         length_measurement_millimeter = length, comment_text, created_datetime,
         created_by, modified_datetime, modified_by)

#==============================================================================================
# Load to database
#==============================================================================================

# Write to location before survey
pg_con = pg_con_local(dbname = "ps_shellfish")
tbl = Id(schema = "public", table = "location")
DBI::dbWriteTable(pg_con, tbl, location, row.names = FALSE, append = TRUE, copy = TRUE)
DBI::dbDisconnect(pg_con)

# Write to location_coordinates before survey
pg_con = pg_con_local(dbname = "ps_shellfish")
tbl = Id(schema = "public", table = "location_coordinates")
DBI::dbWriteTable(pg_con, tbl, location_coordinates, row.names = FALSE, append = TRUE, copy = TRUE)
DBI::dbDisconnect(pg_con)

# Write to person before survey...only if new persons added to database
pg_con = pg_con_local(dbname = "ps_shellfish")
tbl = Id(schema = "public", table = "person")
DBI::dbWriteTable(pg_con, tbl, person, row.names = FALSE, append = TRUE, copy = TRUE)
DBI::dbDisconnect(pg_con)

# Write to survey
pg_con = pg_con_local(dbname = "ps_shellfish")
tbl = Id(schema = "public", table = "survey")
DBI::dbWriteTable(pg_con, tbl, survey, row.names = FALSE, append = TRUE, copy = TRUE)
DBI::dbDisconnect(pg_con)

# Write to survey_person
pg_con = pg_con_local(dbname = "ps_shellfish")
tbl = Id(schema = "public", table = "survey_person")
DBI::dbWriteTable(pg_con, tbl, survey_person, row.names = FALSE, append = TRUE, copy = TRUE)
DBI::dbDisconnect(pg_con)

# Write to survey_event
pg_con = pg_con_local(dbname = "ps_shellfish")
tbl = Id(schema = "public", table = "survey_event")
DBI::dbWriteTable(pg_con, tbl, srv_event, row.names = FALSE, append = TRUE, copy = TRUE)
DBI::dbDisconnect(pg_con)

# Write to species_encounter
pg_con = pg_con_local(dbname = "ps_shellfish")
tbl = Id(schema = "public", table = "species_encounter")
DBI::dbWriteTable(pg_con, tbl, species_encounter, row.names = FALSE, append = TRUE, copy = TRUE)
DBI::dbDisconnect(pg_con)

# Write to individual_species
pg_con = pg_con_local(dbname = "ps_shellfish")
tbl = Id(schema = "public", table = "individual_species")
DBI::dbWriteTable(pg_con, tbl, individual_species, row.names = FALSE, append = TRUE, copy = TRUE)
DBI::dbDisconnect(pg_con)




