#' Title
#'
#' @param df
#' @param squid_aoi
#' @param min_year
#' @param max_year
#' @param min_num
#'
#' @return
#' @export
#'
#' @examples
aggregate_port_visits <- function(df, squid_aoi, min_year = 2017,
                                  max_year = 2020, min_num = 3) {
  df %>%
    filter(year <= {{ max_year }} & year >= {{ min_year }}) %>%
    select(ssvid, aoi, start_anchorage_iso3) %>%
    filter(aoi == squid_aoi) %>%
    distinct(ssvid, aoi, start_anchorage_iso3) %>%
    count(aoi, start_anchorage_iso3) %>%
    filter(n > min_num) %>%
    rename(iso3 = start_anchorage_iso3) %>%
    rbind(df %>%
            filter(year <= {{ max_year }} & year >= {{ min_year }}) %>%
            select(ssvid, aoi, end_anchorage_iso3) %>%
            filter(aoi == squid_aoi) %>%
            distinct(ssvid, aoi, end_anchorage_iso3) %>%
            count(aoi, end_anchorage_iso3) %>%
            filter(n > min_num) %>%
            rename(iso3 = end_anchorage_iso3)) %>%
    group_by(aoi, iso3) %>%
    summarize(n = sum(n, na.rm = TRUE)) %>%
    arrange(desc(n))
}




#' Title
#'
#' @param df
#' @param squid_aoi
#' @param min_year
#' @param max_year
#' @param min_count
#'
#' @return
#' @export
#'
#' @examples
specific_ports_byaoi <- function(df, squid_aoi, min_year = 2017,
                                 max_year = 2020, min_count = 3) {
  df %>%
    filter(year <= {{ max_year }} & year >= {{ min_year }}) %>%
    select(ssvid, aoi, start_anchorage_label, start_anchorage_iso3) %>%
    filter(aoi == {{ squid_aoi }}) %>%
    distinct(ssvid, aoi, start_anchorage_label, start_anchorage_iso3) %>%
    count(aoi, start_anchorage_label, start_anchorage_iso3) %>%
    rename(
      label = start_anchorage_label,
      iso3 = start_anchorage_iso3
    ) %>%
    rbind(df %>%
            filter(year <= {{ max_year }} & year >= {{ min_year }}) %>%
            select(ssvid, aoi, end_anchorage_label, end_anchorage_iso3) %>%
            filter(aoi == {{ squid_aoi }}) %>%
            distinct(ssvid, aoi, end_anchorage_label, end_anchorage_iso3) %>%
            count(aoi, end_anchorage_label, end_anchorage_iso3) %>%
            rename(
              label = end_anchorage_label,
              iso3 = end_anchorage_iso3
            )) %>%
    group_by(aoi, label, iso3) %>%
    summarize(n = sum(n, na.rm = TRUE)) %>%
    filter(n > min_count) %>%
    arrange(iso3)
}



