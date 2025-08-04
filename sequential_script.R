#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Install packages ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# remotes::install_github("PIP-Technical-Team/pipload@dev",
#                         dependencies = FALSE)

# pak::pak("PIP-Technical-Team/wbpip@DEV", ask = FALSE)
# pak::pak("PIP-Technical-Team/wbpip@add_spl_to_dist_functions", ask = FALSE)
# pak::pak("PIP-Technical-Team/pipfun@ongoing", ask = FALSE)
# pak::pak("PIP-Technical-Team/pipload@new_pipeline", ask = FALSE)
# pak::pak("randrescastaneda/joyn@DEV", ask = FALSE)
# pak::pak("randrescastaneda/joyn@upload_values_fix", ask = FALSE)
# pak::pak("PIP-Technical-Team/pipload@ongoing", ask = FALSE)

# remotes::install_github("PIP-Technical-Team/wbpip",
#                        dependencies = FALSE)



#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 1 ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

## Select Defaults ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


py                 <- 2021  # PPP year
branch             <- "main"
branch             <- "DEV"
release            <- "20240627"
release            <- "20250401"
identity           <- "INT"
identity           <- "PROD"
max_year_country   <- 2023
max_year_aggregate <- 2025
max_year_lineup    <- 2023

## filter creation of synth data
cts <- yrs <- NULL

## save data
force_create_cache_file         <- FALSE
save_pip_update_cache_inventory <- FALSE
force_gd_2_synth                <- FALSE
save_mp_cache                   <- FALSE


base_dir <- fs::path("e:/PovcalNet/01.personal/wb384996/PIP/pip_ingestion_pipeline")

## Start up ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Load packages
withr::with_dir(new = base_dir,
                code = {
                  # source("./_packages.R")

                  # Load R files
                  purrr::walk(fs::dir_ls(path = "./R",
                                         regexp = "\\.R$"), source)

                  # Read pipdm functions
                  purrr::walk(fs::dir_ls(path = "./R/pipdm/R",
                                         regexp = "\\.R$"), source)
                })


## Run common R code   ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

base_dir |>
  fs::path("_common.R") |>
  source(echo = FALSE)

# debugonce(from_gd_2_synth)
# debugonce(find_new_svy_data)
base_dir |>
  fs::path("_cache_loading_saving.R") |>
  source(echo = FALSE)



## Set targets options   ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Check that the correct _targets store is used

if (!identical(fs::path(tar_config_get('store')),
               fs::path(gls$PIP_PIPE_DIR, 'pc_data/_targets2021'))) {
  stop('The store specified in _targets.yaml doesn\'t match with the pipeline directory')
}

# filter for testing --------


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 2: Run pipeline   ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~



gd_means <-
get_groupdata_means(cache_inventory = cache_inventory, gdm = dl_aux$gdm)

svy_mean_lcu <-
mp_svy_mean_lcu(cache, gd_means)

svy_mean_lcu_table <-
db_create_lcu_table(dl = svy_mean_lcu, pop_table = dl_aux$pop,
    pfw_table = dl_aux$pfw)

svy_mean_ppp_table <-
db_create_dsm_table(lcu_table = svy_mean_lcu_table, cpi_table = dl_aux$cpi,
    ppp_table = dl_aux$ppp)

dt_ref_mean_pred <-
refy_mean_inc_group(dsm = svy_mean_ppp_table, gls = gls, dl_aux = dl_aux,
    pinv = pipeline_inventory)

lorenz <-
mp_lorenz(cache)

dl_dist_stats <-
mp_dl_dist_stats(dt = cache, mean_table = svy_mean_ppp_table,
    pop_table = dl_aux$pop, cache_id = cache_ids, ppp_year = py)

dt_dist_stats <-
db_create_dist_table(dl = dl_dist_stats, dsm_table = svy_mean_ppp_table,
    crr_inv = cache_inventory)

dt_prod_ref_estimation <-
db_create_ref_estimation_table(ref_year_table = dt_ref_mean_pred,
    dist_table = dt_dist_stats)

dt_prod_svy_estimation <-
db_create_svy_estimation_table(dsm_table = svy_mean_ppp_table,
    dist_table = dt_dist_stats, gdp_table = dl_aux$gdp, pce_table = dl_aux$pce)

dl_coverage <-
db_create_coverage_table(ref_year_table = dt_ref_mean_pred, pop_table = dl_aux$pop,
    cl_table = dl_aux$country_list, incgrp_table = dl_aux$income_groups,
    ref_years = gls$PIP_REF_YEARS, urban_rural_countries = c("ARG",
        "CHN"), digits = 2, gls = gls)

dl_censored <-
db_create_censoring_table(censored = dl_aux$censoring, coverage_list = dl_coverage,
    coverage_threshold = 50)

dt_pop_region <-
db_create_reg_pop_table(pop_table = dl_aux$pop, cl_table = dl_aux$country_list,
    region_code = "pcn_region_code", pip_years = gls$PIP_REF_YEARS)

dt_decomposition <-
db_create_decomposition_table(dsm_table = svy_mean_ppp_table)

all_aux <-
list(dl_aux$cpi, dl_aux$gdp, dl_aux$pop, dl_aux$ppp, dl_aux$pce)

aux_names <-
c("cpi", "gdp", "pop", "ppp", "pce")

aux_clean <- purrr::map2(all_aux, aux_names, \(x,y) {
  db_clean_aux(x, y, pip_years = gls$PIP_YEARS) })


dt_framework <-
create_framework(dl_aux$pfw)

survey_files <-
mp_survey_files(cache = cache, cache_ids = cache_ids, output_dir = gls$OUT_SVY_DIR_PC,
    cols = c("welfare", "weight", "area"), compress = gls$FST_COMP_LVL)

aux_out_files <-
aux_out_files_fun(gls$OUT_AUX_DIR_PC, aux_names)

aux_out <-
fst::write_fst(x = aux_clean, path = aux_out_files, compress = gls$FST_COMP_LVL)


countries_out <-
save_aux_data(dl_aux$countries, fs::path(gls$OUT_AUX_DIR_PC,
    "countries.fst"), compress = TRUE)

missing_data_out <-
save_aux_data(dl_aux$missing_data, fs::path(gls$OUT_AUX_DIR_PC,
    "missing_data.fst"), compress = TRUE)

country_list_out <-
save_aux_data(dl_aux$country_list, fs::path(gls$OUT_AUX_DIR_PC,
    "country_list.fst"), compress = TRUE)

regions_out <-
save_aux_data(dl_aux$regions, fs::path(gls$OUT_AUX_DIR_PC, "regions.fst"),
    compress = TRUE)

country_profiles_out <-
save_aux_data(dl_aux$cp, fs::path(gls$OUT_AUX_DIR_PC, "country_profiles.rds"),
    compress = TRUE)

poverty_lines_out <-
save_aux_data(dl_aux$pl, fs::path(gls$OUT_AUX_DIR_PC, "poverty_lines.fst"),
    compress = TRUE)

national_poverty_lines_out <-
save_aux_data(dl_aux$npl, fs::path(gls$OUT_AUX_DIR_PC, "national_poverty_lines.fst"),
    compress = TRUE)

survey_metadata_out <-
save_aux_data(dl_aux$metadata, fs::path(gls$OUT_AUX_DIR_PC, "survey_metadata.rds"),
    compress = TRUE)

indicators_out <-
save_aux_data(dl_aux$indicators, fs::path(gls$OUT_AUX_DIR_PC,
    "indicators.fst"), compress = TRUE)

pop_region_out <-
save_aux_data(dt_pop_region, fs::path(gls$OUT_AUX_DIR_PC, "pop_region.fst"),
    compress = TRUE)

region_year_coverage_out <-
save_aux_data(dl_coverage$region, fs::path(gls$OUT_AUX_DIR_PC,
    "region_coverage.fst"), compress = TRUE)

incomeGroup_year_coverage_out <-
save_aux_data(dl_coverage$incgrp, fs::path(gls$OUT_AUX_DIR_PC,
    "incgrp_coverage.fst"), compress = TRUE)

country_year_coverage_out <-
save_aux_data(dl_coverage$country_year_coverage, fs::path(gls$OUT_AUX_DIR_PC,
    "country_coverage.fst"), compress = TRUE)

censored_out <-
save_aux_data(dl_censored, fs::path(gls$OUT_AUX_DIR_PC, "censored.rds"),
    compress = TRUE)

decomposition_out <-
save_aux_data(dt_decomposition, fs::path(gls$OUT_AUX_DIR_PC,
    "decomposition.fst"), compress = TRUE)

framework_out <-
save_aux_data(dt_framework, fs::path(gls$OUT_AUX_DIR_PC, "framework.fst"),
    compress = TRUE)

dictionary_out <-
save_aux_data(dl_aux$dictionary, fs::path(gls$OUT_AUX_DIR_PC,
    "dictionary.fst"), compress = TRUE)

prod_ref_estimation_file <-
save_estimations(dt = dt_prod_ref_estimation, dir = gls$OUT_EST_DIR_PC,
    name = "prod_ref_estimation", time = gls$TIME, compress = gls$FST_COMP_LVL)

prod_svy_estimation_file <-
save_estimations(dt = dt_prod_svy_estimation, dir = gls$OUT_EST_DIR_PC,
    name = "prod_svy_estimation", time = gls$TIME, compress = gls$FST_COMP_LVL)

lorenz_out <-
save_aux_data(lorenz, fs::path(gls$OUT_AUX_DIR_PC, "lorenz.rds"),
    compress = TRUE)

dist_file <-
save_estimations(dt = dt_dist_stats, dir = gls$OUT_EST_DIR_PC,
    name = "dist_stats", time = gls$TIME, compress = gls$FST_COMP_LVL)

survey_mean_file <-
save_estimations(dt = svy_mean_ppp_table, dir = gls$OUT_EST_DIR_PC,
    name = "survey_means", time = gls$TIME, compress = gls$FST_COMP_LVL)

survey_mean_file_aux <-
save_estimations(dt = svy_mean_ppp_table, dir = gls$OUT_AUX_DIR_PC,
    name = "survey_means", time = gls$TIME, compress = gls$FST_COMP_LVL)

aux_versions_out <-
save_estimations(dt = aux_versions, dir = gls$OUT_AUX_DIR_PC,
    name = "aux_versions", time = gls$TIME, compress = gls$FST_COMP_LVL)

interpolated_means_file <-
save_estimations(dt = dt_ref_mean_pred, dir = gls$OUT_EST_DIR_PC,
    name = "interpolated_means", time = gls$TIME, compress = gls$FST_COMP_LVL)

interpolated_means_file_aux <-
save_estimations(dt = dt_ref_mean_pred, dir = gls$OUT_AUX_DIR_PC,
    name = "interpolated_means", time = gls$TIME, compress = gls$FST_COMP_LVL)

metaregion_file_aux <-
save_estimations(dt = dl_aux$metaregion, dir = gls$OUT_AUX_DIR_PC,
    name = "metaregion", time = gls$TIME, compress = gls$FST_COMP_LVL)

data_timestamp_file <-
writeLines(as.character(Sys.time()), fs::path(gls$OUT_DIR_PC,
    gls$vintage_dir, "data_update_timestamp", ext = "txt"))

aux_qs_out <-
convert_to_qs(dir = gls$OUT_AUX_DIR_PC)

