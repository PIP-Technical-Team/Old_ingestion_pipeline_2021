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
release            <- "20250401"
release            <- "20250930"
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
# base_dir |>
#   fs::path("_cache_loading_saving.R") |>
#   source(echo = FALSE)

# pipeline_inventory <-
#   pipeline_inventory[module  != "PC-GROUP"]




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

list(
  # AUX data ------------------

  tar_target(aux_tb,
             prep_aux_data(maindir = gls$PIP_DATA_DIR,
                           branch  = branch)),

  # Load aux data
  tar_target(dl_aux1,
             load_aux_data(aux_tb),
             cue = tar_cue(mode = "always") ),

  # format data
  tar_target(dl_aux,
             format_aux_data(dl_aux1, py)),


  # CACHE data ------------

  # Load PIP inventory
  tar_target(pip_inventory_file,
             fs::path(gls$PIP_DATA_DIR, '_inventory/inventory.fst'),
             format = "file"),

  tar_target(pip_inventory,
             load_pip_inventory(pip_inventory_file)),

  # Load PIPELINE inventory file
  tar_target(pipeline_inventory,
               db_filter_inventory(dt        = pip_inventory,
                                   pfw_table = dl_aux$pfw) |>
               _[module  != "PC-GROUP"]),


  # Create microdata cache files
  tar_target(status_cache_files_creation,
               create_cache_file(
                 pipeline_inventory = pipeline_inventory,
                 pip_data_dir       = gls$PIP_DATA_DIR,
                 tool               = "PC",
                 cache_svy_dir      = gls$CACHE_SVY_DIR_PC,
                 compress           = gls$FST_COMP_LVL,
                 force              = force_create_cache_file,
                 verbose            = TRUE,
                 cpi_table          = dl_aux$cpi,
                 ppp_table          = dl_aux$ppp,
                 pfw_table          = dl_aux$pfw,
                 pop_table          = dl_aux$pop)),

  # Create synthetic cache files
  tar_target(pipeline_inventory2,
               from_gd_2_synth(dl_aux             = dl_aux,
                               gls                = gls,
                               pipeline_inventory = pipeline_inventory,
                               force              = force_gd_2_synth,
                               cts                = cts,
                               yrs                = yrs)),
  tar_target(cache_inventory1,
               pip_update_cache_inventory(
                 pipeline_inventory = pipeline_inventory2,
                 pip_data_dir       = gls$PIP_DATA_DIR,
                 cache_svy_dir      = gls$CACHE_SVY_DIR_PC,
                 tool               = "PC",
                 save               = save_pip_update_cache_inventory,
                 load               = TRUE,
                 verbose            = TRUE
               )),

  # cache IDs
  tar_target(cache_ppp, gls$cache_ppp),

  # filter cache inventory with PFW
  tar_target(cache_inventory,
             filter_cache_inventory(cache_inventory1, dl_aux)),


  tar_target(cache_ids,
             get_cache_id(cache_inventory)),
  tar_files(cache_dir,
             get_cache_files(cache_inventory) |>
               setNames(cache_ids)),


  # create cache global list
  tar_target(cache_file,
             create_cache(cache_dir = cache_dir,
                          cache_ids = cache_ids,
                          save = FALSE,
                          gls = gls,
                          cache_ppp = cache_ppp),
             format = "file"),

  # Load cache file
  tar_target(cache,
             load_cache(cache_file)),
  tar_target(assert_cache_length,
             tar_cancel(length(cache) == length(cache_dir)))


)

