
#' Run queries to load key datasets
#'
#' @param type
#'
#' @return
#' @export
#'
#' @examples
get_connections_data <- function(type) {
  if (type == 'squid') {
    bqtable = 'paper_global_squid.light_luring_by_region_2017_2020_v20220525'
  } else if (type == 'carrier') {
    bqtable = 'paper_global_squid.carrier_positions_in_squidregions_v20220525'
  } else if (type == 'squid_ports') {
    bqtable = 'paper_global_squid.squid_vessel_port_visits_withaoi_2017_2020_v20220525'
  } else if (type == 'carrier_ports') {
    bqtable = 'paper_global_squid.carrier_vessel_port_visits_withaoi_2017_2020_v20220525'
  } else {
    stop('type must be "squid", "carrier", "squid_ports", "carrier_ports"')
  }

  fishwatchr::gfw_query(
    query = glue::glue("
  SELECT
   *
  FROM
  {bqtable}"),
    save_query = FALSE,
    run_query = TRUE,
    con = con
  )$data
}


#' Save plots
#'
#' @param plot_list
#' @param file_name
#' @param plot_width
#' @param plot_height
#' @param units
#' @param dpi
#'
#' @return
#' @export
#'
#' @examples
save_plots <- function(plot_list, file_name, plot_width, plot_height,
                       units = "mm", dpi = 300, output_type, ...) {
  dots <- list(...)

  if (output_type == 'pdf') {
    device = cairo_pdf
  } else {
    device = NULL
  }

  ggplot2::ggsave(plot_list,
                  filename = here::here("outputs","figures",dots$figure_version,glue::glue("{file_name}.{output_type}")),
                  width = plot_width,
                  height = plot_height,
                  units = units,
                  dpi = dpi,
                  device = device
  )
}
