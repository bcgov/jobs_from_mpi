select_stuff <- function(tbbl){
  tbbl|>
    select(project_id=contains("_id"),
           project_name= any_of(c("project_name","proj_nm")),
           estimated_cost=contains("_cost"),
           construction_type= any_of(c("proj_cons_typ","construction_type")),
           region,
           project_status=contains("status"))
}


Mode <- function(x) {
  # calculates the most common value of a vector
  ux <- unique(x)
  ux[which.max(tabulate(match(x, ux)))]
}

clean_values <- function(tbbl){
  tbbl|>
    mutate(project_id=as.numeric(str_extract(project_id, "\\d+")),
           estimated_cost=as.numeric(str_extract(estimated_cost, "\\d+")),
           project_status=str_remove_all(project_status, "Status: ")
    )
}

mode_fill <- function(tbbl) {
  # over-writes variables that SHOULD be constant with the project's modal (most common) value.
  tbbl %>%
    mutate(
      project_name = Mode(project_name),
      region = Mode(region),
      construction_type = Mode(construction_type))
}

updown_fill <- function(tbbl) {
  all_quarters <- tibble(
    date = seq(min(tbbl$date, na.rm = TRUE),
                      max(tbbl$date, na.rm = TRUE),
                      by = "quarter"
    )
  )
 tbbl <- left_join(all_quarters, tbbl, by = c("date" = "date"))%>%
    fill(estimated_cost, .direction = "updown") %>%
    fill(project_status, .direction='updown')
}

calc_UR_rc <- function(region, tbbl){
  tbbl|>
    filter(bc_region==region,
           naics_5 %in% construction_naics
    )|>
    group_by(date, lf_stat)|>
    summarise(count=sum(count))|>
    pivot_wider(names_from = lf_stat, values_from = count)|>
    mutate(UR_rc=Unemployed/(Employed+Unemployed))|>
    select(date, UR_rc)
}

calc_UR_r_notc <- function(region, tbbl){
  tbbl|>
    filter(bc_region==region,
           ! naics_5 %in% construction_naics
    )|>
    group_by(date, lf_stat)|>
    summarise(count=sum(count))|>
    pivot_wider(names_from = lf_stat, values_from = count)|>
    mutate(UR_r_notc=Unemployed/(Employed+Unemployed))|>
    select(date, UR_r_notc)
}

calc_UR_notr_c <- function(region, tbbl){
  tbbl|>
    filter(bc_region!=region,
           naics_5 %in% construction_naics
    )|>
    group_by(date, lf_stat)|>
    summarise(count=sum(count))|>
    pivot_wider(names_from = lf_stat, values_from = count)|>
    mutate(UR_notr_c=Unemployed/(Employed+Unemployed))|>
    select(date, UR_notr_c)
}
join_wrapper <- function(tbbl1, tbbl2, tbbl3){
  tbbl1|>
    full_join(tbbl2)|>
    full_join(tbbl3)
}



