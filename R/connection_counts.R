
#' Count of connections between regions
#'
#' @param df squid vessel postions, dataframe
#' @param combinations unique region combinations, list
#'
#' @return dataframe with counts of connections for each region combination
#' @export
#'
#' @examples
region_connection_count <- function(df, combinations) {
  df %>%
    select(ssvid, aoi) %>%
    filter(aoi == combinations[1]) %>%
    inner_join(
      df %>%
        select(ssvid, aoi) %>%
        filter(aoi == combinations[2]),
      by = c("ssvid" = "ssvid")
    ) %>%
    distinct() %>%
    summarize(
      region_pair = glue::glue("{aoi.x}:{aoi.y}"),
      connections = n()
    ) %>%
    distinct()
}





#' Count vessels not found in another region
#'
#' @param df squid vessel positions, dataframe
#' @param region squid fishing regions, string
#'
#' @return
#' @export
#'
#' @examples
endemic_vessel_counts <- function(df, region) {

  # identify regions ending in 'hs' and not
  # starting with 'nc' or 'eq', and remove 'hs'
  region <- ifelse(grepl(
    pattern = "^(?!nc|eq).*(_hs)$",
    x = region,
    perl = TRUE
  ) == TRUE,
  gsub(
    pattern = "(_hs)$",
    replacement = "",
    region
  ),
  region
  )

  endemic_cnt <- df %>%
    filter(aoi %in% {{ region }}) %>%
    filter(!ssvid %in% c(df %>%
                           filter(!aoi %in% c({{ region }})) %>%
                           dplyr::distinct(ssvid) %>%
                           pull(ssvid))) %>%
    distinct(ssvid) %>%
    nrow(.)

  total_cnt <- df %>%
    filter(aoi %in% c({{ region }})) %>%
    distinct(ssvid) %>%
    nrow(.)

  return(tibble::tibble(
    num_endemic = endemic_cnt,
    total = total_cnt,
    frac_endemic = endemic_cnt / total_cnt
  ))
}

#' Count vessels one or more regions
#'
#' @param df squid vessel positions, dataframe
#' @param num_regions number of regions, integer
#'
#' @return
#' @export
#'
#' @examples
ssvid_mult_region <- function(df, num_regions) {
  vessel_cnt <- df %>%
    distinct(ssvid) %>%
    filter(ssvid %in%
      c(df %>%
        distinct(ssvid, aoi) %>%
        count(ssvid, sort = TRUE) %>%
        filter(n == {{ num_regions }}) %>%
        pull(ssvid))) %>%
    nrow(.)

  total_cnt <- df %>%
    distinct(ssvid) %>%
    nrow(.)

  return(tibble::tibble(
    num_regions = num_regions,
    num_ssvid = vessel_cnt,
    total = total_cnt,
    frac_ssvid = vessel_cnt / total_cnt
  ))
}
