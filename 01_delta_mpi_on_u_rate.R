# Copyright 2024 Province of British Columbia
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and limitations under the License.

#' Here we look at whether or not changes in the total estimated cost of
#' "construction started" MPI projects have a lagged effect on the unemployment rate in the
#' construction industry for the 8 economic regions of BC.  We control for:
#' 1) The regions' unemployment rate for all other industries.
#' 2) The construction industry's unemployment rate for all other regions.
#'
#' It appears that lagged changes in the total estimated cost of "construction started" projects
#' do not have a significant impact on the unemployment rate of the construction industry.

# libraries--------
library(tidyverse)
library(here)
library(fpp3)
library(janitor)
library(dynlm)
library(conflicted)
conflicts_prefer(dplyr::filter)
conflicts_prefer(plm::lag)
#functions-------------------
source(here::here("R","functions.R"))

#get the mpi data---------------------------------------

mpi_url_to_scrape <- "https://www2.gov.bc.ca/gov/content/employment-business/economic-development/industry/bc-major-projects-inventory/recent-reports"
mpi_scraped <- rvest::read_html(mpi_url_to_scrape)
dirty_links <- rvest::html_attr(rvest::html_nodes(mpi_scraped, "a"), "href")
mpi_links <- unique(dirty_links[str_detect(dirty_links, ".xls")])
mpi_years <- str_extract(mpi_links, "\\d{4}")
quarter_input <- str_extract(mpi_links, "q\\d{1}")
quarters_as_months <- case_when(
  quarter_input == "q1" ~ "mar",
  quarter_input == "q2" ~ "jun",
  quarter_input == "q3" ~ "sep",
  quarter_input == "q4" ~ "dec"
)|>
  na.omit()

month_names <- str_to_lower(paste0("(",paste(c(month.abb, month.name), collapse = "|"),")"))
mpi_months <- str_extract(mpi_links, month_names)|>
  na.omit()

mpi_months <- c(quarters_as_months, mpi_months)
mpi_dates<- paste0(mpi_years, mpi_months)
mpi_extensions <- tools::file_ext(mpi_links)
mpi_files <- paste0(mpi_dates, ".", mpi_extensions) # sane file naming.
# NOTE THAT YOU ONLY NEED TO DOWNLOAD THE DATA ONCE PER QUARTER... FOLLOWING LINE CAN BE COMMENTED OUT TO SKIP DOWNLOAD
# mapply(download.file, mpi_links, here::here("raw_data", mpi_files), mode="wb") # NOTE MODE="WB" FOR WINDOZ COMPATABILITY
########
mpi_all_sheets <- sapply(here::here("raw_data", mpi_files), readxl::excel_sheets) # gets all the sheets
sheet_starts_with_mpi <- lapply(mpi_all_sheets, function(x) x[startsWith(x, "mpi")]) |>
  unlist(use.names = FALSE) # could break... assumes excel sheet naming remains consistent.
sheet_starts_with_full <- lapply(mpi_all_sheets, function(x) x[startsWith(x, "Full")]) |>
  unlist(use.names = FALSE) # could break... assumes excel sheet naming remains consistent.
number_first_sheets <- length(mpi_all_sheets)-length(sheet_starts_with_mpi)-length(sheet_starts_with_full)
first_sheet <- lapply(mpi_all_sheets, head, n=1) |>
  unlist(use.names = FALSE) # could break... assumes excel sheet naming remains consistent.
first_sheet <- tail(first_sheet, n=number_first_sheets)
mpi_sheets <- c(sheet_starts_with_mpi, sheet_starts_with_full, first_sheet)
mpi_skip= c(rep(0, length(mpi_sheets)-length(first_sheet)), rep(3, length(first_sheet)))

mpi <- tibble(date=ym(mpi_dates), path = here::here("raw_data", mpi_files), sheet = mpi_sheets, skip=mpi_skip)|>
  mutate(data = pmap(list(path, sheet, skip), ~ readxl::read_excel(path = ..1, sheet = ..2, skip = ..3)),
         data = map(data, janitor::clean_names),
         data = map(data, select_stuff),
         data = map(data, clean_values))|>
  select(date, data)|>
  unnest(data)|>
  group_by(project_id)|>
  nest()|>
  mutate(data = map(data, mode_fill)) |> # replace all the categorical variables that SHOULD be constant with their modal value
  unnest(data)|>
  group_by(project_id, #all the non variable variables
           project_name,
           construction_type,
           region) |>
  nest()|>
  mutate(data = map(data, updown_fill))|>
  unnest(data)|>
  mutate(region=str_replace_all(region, "/","-"))|>
  filter(!project_id %in% c(3279, 293))

diff_cost <- mpi|>
  filter(project_status %in% c("Construction started", "Completed"))|>
  mutate(bc_region=str_sub(region, 4))|>
  group_by(date, bc_region)|>
  summarize(estimated_cost=sum(estimated_cost, na.rm=TRUE))|>
  group_by(bc_region)|>
  mutate(mpi_diff=c(NA_real_, diff(estimated_cost)),
         date=yearquarter(date)
         )|>
  na.omit()|>
  select(-estimated_cost)
#visualize-----------------------------------------
ggplot(diff_cost, aes(date, mpi_diff))+
  geom_line()+
  scale_y_continuous(labels=scales::dollar)+
  facet_grid(~bc_region, scales="free")+
  labs(x=NULL,
       y="Difference in Estimated Cost (M)",
       title="Shocks to the Labour Market: Quarterly Difference in MPI Estimated Cost by Region",
       subtitle="Spikes: TMX cost overruns (Cariboo), Smelter Modernization Project & LNG (North Coast), Site C & GasLink (Northeast)")

# construction employment/unemployment-------------------------

construction_naics <- 2361:2389

status <- vroom::vroom(here("raw_data","rtra", list.files(here("raw_data","rtra"))))|>
  na.omit()|>
  clean_names()|>
  mutate(naics_5=as.numeric(naics_5),
         date=tsibble::yearquarter(ym(paste(syear, smth, sep="-"))),
         bc_region=if_else(bc_region=="Lower Mainland-Southwest", "Mainland-Southwest", bc_region),
         bc_region=if_else(bc_region=="Vancouver Island and Coast", "Vancouver Island-Coast", bc_region)
  )|>
  filter(lf_stat!="Unknown",
         date<tsibble::yearquarter(ymd("20241201")))

unemployment_rates <- tibble(bc_region=unique(status$bc_region), lfs=list(status))|>
  mutate(UR_rc=map2(bc_region, lfs, calc_UR_rc),
         UR_r_notc=map2(bc_region, lfs, calc_UR_r_notc),
         UR_notr_c=map2(bc_region, lfs, calc_UR_notr_c),
         UR=pmap(list(UR_rc, UR_r_notc, UR_notr_c), join_wrapper)
  )|>
  select(bc_region, UR)|>
  unnest(UR)

#fit the model------------------------------------------

pdata <- inner_join(diff_cost, unemployment_rates)|>
  pdata.frame(index = c("bc_region", "date"))

# Fit panel ADL model with lags
panel_adl_model <- plm(
  UR_rc ~ UR_r_notc+ UR_notr_c + lag(mpi_diff, 0:4),
  data = pdata,
  model = "pooling"
)

summary(panel_adl_model)


