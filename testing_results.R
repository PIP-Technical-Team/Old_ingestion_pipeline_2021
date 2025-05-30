# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# project:       TEsting results
# Author:        Andres Castaneda
# Dependencies:  The World Bank
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Creation Date:    2022-06-03
# Modification Date:
# Script version:    01
# References:
#
#
# Output:             tables
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#                   Load Libraries   ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# pak::pak("PIP-technical-team/pipapi@DEV")
# pak::pak("PIP-technical-team/wbpip@DEV")
# pak::pak("PIP-technical-team/pipapi@PROD")

library(fastverse)
library(ggplot2)
library(pipapi)
options(pipapi.query_live_data = TRUE)
getOption("pipapi.query_live_data")

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#                   Subfunctions   ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#                   Set up   ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


v1 <- "20230919_2017_01_02_PROD"
v2 <- "20240326_2017_01_02_PROD"
v2 <- "20240429_2017_01_02_INT"
v2 <- "20240627_2017_01_02_PROD"
v2 <- "20250401_2021_01_02_PROD"

data_pipeline <-  fs::path("//w1wbgencifs01/pip/pip_ingestion_pipeline/pc_data/output-tfs-sync/ITSES-POVERTYSCORE-DATA")

data_pipeline <-  fs::path("e:/PIP/pipapi_data/")

lkups <- create_versioned_lkups(
  data_pipeline,
  # vintage_pattern = "(20240627|20240429)_2017_01_02"
  vintage_pattern = "20250401"
  # vintage_pattern = "2017_01_02_INT"
  )

lkup <- lkups$versions_paths[[lkups$latest_release]]
lkup <- lkups$versions_paths$`20250401_2021_01_02_PROD`

# Compare two different version -----------

## survey data -----------

ctr <- "all"
pl <- 3


# pip1_cl   <- pipr::get_stats(povline = pl)
# setDT(pip1_cl)
#
# setorderv(pip1_cl,  c("country_code", "reporting_level", "year"))

pip2_cl   <- pipapi::pip(country = ctr,
                     lkup = lkup,
                     povline = pl)


setnames(pip2_cl, "reporting_year", "year")

setorderv(pip2_cl,  c("country_code", "year", "reporting_level"))


x <- "headcount"

x <- c("headcount", "poverty_gap", "poverty_severity", "watts", "mean", "median", "mld", "gini", "polarization", "decile1", "decile10", "cpi", "ppp","spl", "spr")

lapply(x, \(.) {
  vars <- c("year", .)
  waldo::compare(pip1_cl[country_code == "NAM", ..vars],
                 pip2_cl[country_code == "NAM",..vars],
                 tolerance = 1e-6)

})


rf <- lkup$ref_lkup |> copy()











#  waldo::compare(pip1, pip2)


## lineup data ----------
pip1   <- pipr::get_stats(povline = pl,
                          fill_gaps = TRUE) |>
  frename(year = reporting_year) |>
  setorder(country_code, reporting_year, reporting_level, welfare_type) |>
  qDT()

# pip2   <- pipapi::pip(country = ctr,
#                       fill_gaps = TRUE,
#                       lkup = lkups$versions_paths[[v2]],
#                       povline = pl)  |>
#   setorder(country_code, reporting_year, reporting_level, welfare_type)


pip2   <- pipapi::pip(country = ctr,
                      fill_gaps = TRUE,
                      lkup = lkup,
                      povline = pl)  |>
  setorder(country_code, reporting_year, reporting_level, welfare_type)


nga <- pipapi::pip(country = "NGA",
                   fill_gaps = TRUE,
                   lkup = lkup,
                   povline = 3.65)  |>
  setorder(country_code, reporting_year, reporting_level, welfare_type)



pip2cy   <- pipapi::pip(country = ctr,
                      fill_gaps = FALSE,
                      lkup = lkup,
                      povline = pl)  |>
  setorder(country_code, reporting_year, reporting_level, welfare_type)

nga   <- pipapi::pip(country = "NGA",
                      fill_gaps = FALSE,
                      lkup = lkup,
                      povline = pl)  |>
  setorder(country_code, reporting_year, reporting_level, welfare_type)

# debugonce(pipapi:::fg_pip)
debugonce(pipapi:::fg_remove_duplicates)
idnn   <- pipapi::pip(country = "IDN",
                      fill_gaps = TRUE,
                      lkup = lkup,
                      povline = pl,
                     year = c(1988, 1991))  |>
  setorder(country_code, reporting_year, reporting_level, welfare_type)


idno   <- pipapi::pip(country = "IDN",
                      fill_gaps = TRUE,
                      lkup = lkups$versions_paths$`20240326_2017_01_02_PROD`,
                      povline = pl,
                     year = c(1988, 1991))  |>
  setorder(country_code, reporting_year, reporting_level, welfare_type)






df <- joyn::joyn(pip1[country_code == "KOR" & reporting_year > 2010
                      , .(reporting_year, mean)],
                 pip2[country_code == "KOR" & reporting_year > 2010
                      , .(reporting_year, mean)],
                 by = "reporting_year",
                 keep_common_vars = TRUE,
                 reportvar = FALSE) |>
  pivot("reporting_year", names = list("version", "mean")) |>
  ftransform(version = fifelse(version == "mean.x", "old", "new"))









ggplot(df, aes(x = reporting_year)) +
  geom_line(aes(y = mean, color = version)) +
  geom_point(aes(y = mean, color = version)) +
  theme_minimal()


## Aggregate data ---------------
pip2_g   <- pipapi::pip_grp_logic(country = ctr,
                     lkup = lkup,
                     povline = pl,
                     group_by = "wb")
setnames(pip2_g, "reporting_year", "year")
setorder(pip2_g, region_code, year)

## Aggregate data ---------------
pip2_g   <- pipapi::pip_grp_logic(country = ctr,
                     lkup = lkup,
                     povline = pl,
                     group_by = "wb",
                     year = 2020:2024)
setnames(pip2_g, "reporting_year", "year")
setorder(pip2_g, region_code, year)

# pip2_g   <- pipapi::pip_grp_logic(country = ctr,
#                      lkup = lkups$versions_paths[[v2]],
#                      povline = pl,
#                      group_by = "wb")
#

pip1_g   <- pipr::get_wb(povline = pl)
setDT(pip1_g)
setorder(pip1_g, region_code, year)


waldo::compare(pip1_g[region_code == "WLD", .(year, headcount)],
               pip2_g[region_code == "WLD", .(year, headcount)])



# waldo::compare(pip1, pip2)

# pip2 |>
#   fsubset(reporting_year >= 2019 & country_code == "IND") |>
#   fselect(reporting_year, reporting_level, gini)


### specific countries ------

vars <- c("country_code", "reporting_year", "reporting_level", "welfare_type", "mean", "median", "gini", "headcount")

waldo::compare(pip1[country_code == "SYR",
                    ..vars],
               pip2[country_code == "SYR",
                    ..vars])


waldo::compare(pip1[country_code != "SYR"],
               pip2[country_code != "SYR"])

cct <- "IND"

waldo::compare(pip1[country_code %in% cct],
               pip2[country_code %in% cct])


waldo::compare(pip1[!country_code %in% cct],
               pip2[!country_code %in% cct])



waldo::compare(pip1[country_code %in% cct],
               pip2[country_code %in% cct])


waldo::compare(pip1[!country_code %in% cct],
               pip2[!country_code %in% cct])

# Aggregate data ------------

agg2   <- pipapi::pip_grp_logic(povline = 2.15,
                                lkup = lkups$versions_paths[[v2]],
                                group_by         =  c("wb"))

## max year per region

agg2[, .SD[which.max(reporting_year)],
     by = region_code
     ][,
       .(region_code, reporting_year)
       ]

## values for one region ---------
agg2[reporting_year >= 2015 & region_code == "SAS"
][,
  .(reporting_year, headcount)
]

# testing single release ----------
ctr <- "CHN"

v1 <- "20230626_2017_01_02_TEST"
df   <- pipapi::pip (country = ctr,
                     fill_gaps = TRUE,
                     lkup = lkups$versions_paths[[v1]])



chn20 <-
  purrr::map(.x = seq(from = 1, to = 4, by = .1),
             .f = ~{
               pipapi::pip (country = "CHN",
                            povline = .x,
                            fill_gaps = FALSE,
                            lkup = lkups$versions_paths[[v1]])
             }) |>
  rbindlist(use.names = TRUE)


## chart -----
ggplot(chn20[reporting_year == c(2015, 2017, 2019, 2020)
             & poverty_line > 1.2 & poverty_line < 3.8],
       aes(x = poverty_line,
           y = headcount,
           color = reporting_level)
       ) +
  geom_line() +
  facet_wrap(vars(reporting_year),
             nrow = 2,
             scales = "free_y") +
  theme_minimal() +
  theme(
    legend.position="bottom",
    legend.title = element_blank(),
    panel.spacing = unit(0, "lines"),
    strip.text.x = element_text(size = 8),
    plot.title = element_text(size=13)
  )



# Lined up median ----------

tar_load(dt_lineup_median)

nr <- nrow(dt_lineup_median)
pt <- .01

smp <- sample(1:nr, floor(nr*pt), replace = FALSE)

dl <-
  dt_lineup_median |>
  # fsubset(country_code == "CHN" & reporting_year == 1981) |>
  # ss(smp) |>
  fselect(country = country_code,
          year    = reporting_year,
          povline = median,
          reporting_level) |>
  as.list()



pip_med <-
  purrr::pmap(dl, \(country, year, povline, reporting_level) {
    y <-
      pipapi::pip(country = country,
                year = year,
                povline = povline,
                lkup = lkups$versions_paths[[v1]],
                fill_gaps = TRUE,
                reporting_level = reporting_level)
    }
    ) |>
  rbindlist()

pipbk <- copy(pip_med)

pip_med <- pip_med |>
fselect(country_code,
        reporting_year,
        reporting_level,
        poverty_line,
        headcount)

# save

pip_med[,
        diff_med := abs(.5 - headcount)
        ][,
          country_group := fifelse(country_code %in% c("CHN", "IND", "IDN"),
                                   "CHN-IND-IDN",
                                   "other")]
setorder(pip_med, -diff_med)


tdirp <- "P:/02.personal/wb384996/temporal/R"
haven::write_dta(pip_med, fs::path(tdirp, "lnp_med.dta") )


ggplot(pip_med,
       aes(x = headcount,
           y = diff_med)) +
  geom_point(aes(color = country_group))




plt <- ggbetweenstats(
  data = pip_med[country_group == "other"],
  x = country_group,
  y = diff_med
)

plt



# testing Nishant medians -----------

dta <- fs::path(tdirp, "Country_mean_median_sep23.dta") |>
dta <- fs::path(tdirp, "1kbins_medians.dta") |>
  haven::read_dta()


dla <- dta |>
  fselect(country = country_code ,
          povline = poverty_line,
          year    = reporting_year,
          welfare_type ) |>
  as.list()


dla_med <-
  purrr::pmap(dla, \(country, year, povline, welfare_type) {
    y <-
      pipapi::pip(country = country,
                  year = year,
                  povline = povline,
                  lkup = lkups$versions_paths[[v1]],
                  fill_gaps = TRUE,
                  welfare_type = welfare_type)
  }
  ) |>
  rbindlist()

dlabk <- copy(dla_med)

dla_med <- dla_med |>
  fselect(country_code,
          reporting_year,
          reporting_level,
          poverty_line,
          headcount)

# save

dla_med[,
        diff_med := abs(.5 - headcount)
][,
  country_group := fifelse(country_code %in% c("CHN", "IND", "IDN"),
                           "CHN-IND-IDN",
                           "other")]
setorder(dla_med, -diff_med)



# haven::write_dta(dla_med, fs::path(tdirp, "lnp_med_nishant.dta") )
haven::write_dta(dla_med, fs::path(tdirp, "lnp_med_raw1kbins.dta") )




ggplot(dla_med,
       aes(x = headcount,
           y = diff_med)) +
  geom_point(aes(color = country_group))




dla_plt <- ggbetweenstats(
  data = dla_med,
  x = country_group,
  y = diff_med
)

dla_plt

dla_pltb <- ggbetweenstats(
  data = dla_med[country_group == "other"],
  x = country_group,
  y = diff_med
)

dla_pltb






dla_med[country_group == "other"] |>
  head(50)


# merge and compare -----------


m1kb <- copy(dla_med)
mlnp <- copy(pip_med)


orig_names <-  c("poverty_line", "headcount", "diff_med")
m1kb_names <- paste0("m1kb_", orig_names)
mlnp_names <- paste0("mlnp_", orig_names)

setnames(m1kb, orig_names, m1kb_names)
setnames(mlnp, orig_names, mlnp_names)

m1kb[, survey_year := NULL]
mlnp[, survey_year := NULL]

by_vars <- c("country_code",
             "reporting_year",
             "reporting_level",
             "country_group")


## remove duplicates in CHN, IND and IDN ======
m1kb[,
     min_diff := min(m1kb_diff_med),
     by = by_vars
     ]
m1kb <- m1kb[min_diff == m1kb_diff_med
         ][, min_diff := NULL]

m1kb <- unique(m1kb, by = c(by_vars, "m1kb_diff_med"))


mdt <-
  joyn::joyn(mlnp,
              m1kb,
              by = by_vars,
              match_type = "1:1")


mdt[, diff_hc := abs(mlnp_headcount - m1kb_headcount)]
setorder(mdt, -diff_hc, na.last = TRUE)

norder <- c(by_vars,
  "m1kb_headcount",
  "mlnp_headcount",
  "diff_hc",
  "m1kb_diff_med",
  "mlnp_diff_med",
  "m1kb_poverty_line",
  "mlnp_poverty_line",
  "report"
)


setcolorder(mdt, norder)

mdt_l <-
  melt(mdt,
       id.vars = c(by_vars, "report"),
       measure = patterns("headcount|diff_med|poverty")
  ) |>
  ftransform(source = gsub("([^_]+)(_.*)", "\\1", variable),
             variable = gsub("([^_]+)_(.*)", "\\2", variable)) |>
  fsubset(!is.na(value)) |>
  dcast(... ~ variable, value.var = "value")


setorder(mdt_l, -diff_med)

mdt_pltb <- ggbetweenstats(
  data = mdt_l[country_group == "other" & report == "x & y"],
  x = source,
  y = diff_med
)

mdt_pltb



S
