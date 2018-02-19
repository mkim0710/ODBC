function.list_TABLE_NAME.sqlFetch_sqlColumns = function(channel) {
    # source("https://github.com/mkim0710/ODBC/raw/master/library(RODBC) Big Data Management 2018 HW1 Grading.r")
    library(RODBC)
    library(tidyverse)
    out = filter(sqlTables(channel), TABLE_TYPE == "TABLE")$TABLE_NAME %>% map(function(x) {
        tmp.list = list()
        tmp.list$sqlFetch = sqlFetch(channel, x)
        tmp.list$sqlColumns = sqlColumns(channel, x)
        tmp.list
    })
    names(out) = filter(sqlTables(channel), TABLE_TYPE == "TABLE")$TABLE_NAME
    out
}

solution.list_TABLE_NAME.sqlFetch_sqlColumns = function.list_TABLE_NAME.sqlFetch_sqlColumns(odbcDriverConnect(sprintf("Driver={Microsoft Access Driver (*.mdb, *.accdb)}; DBQ=%s", path_filename )))

function.list_TABLE_NAME.sqlFetch_sqlColumns.df_COLUMN_TYPE = function(channel.list_TABLE_NAME.sqlFetch_sqlColumns.df_COLUMN_TYPE) {
    # source("https://github.com/mkim0710/ODBC/raw/master/library(RODBC) Big Data Management 2018 HW1 Grading.r")
    channel.list_TABLE_NAME.sqlFetch_sqlColumns.df_COLUMN_TYPE %>% map(function(ls) {
        ls$sqlColumns %>% mutate(NULLABLE_LABEL = ifelse(NULLABLE == 0, "NOT_NULLABLE", "NULLABLE")) %>% select(COLUMN_NAME, TYPE_NAME, NULLABLE_LABEL)
    }) %>% bind_rows(.id = "tbl")
}

HW1.list.files = list.files(HW1.path, pattern = "\\.accdb$")
students.list_TABLE_NAME.sqlFetch_sqlColumns = HW1.list.files %>% map(function(x) {
    odbcCloseAll()
    path_filename = paste0(HW1.path, x)
    channel = odbcDriverConnect(sprintf("Driver={Microsoft Access Driver (*.mdb, *.accdb)}; DBQ=%s", path_filename ))
    out = function.list_TABLE_NAME.sqlFetch_sqlColumns(channel)
    
})
names(students.list_TABLE_NAME.sqlFetch_sqlColumns) = HW1.list.files


students.list_TABLE_NAME.sqlFetch_sqlColumns.df_COLUMN_TYPE = students.list_TABLE_NAME.sqlFetch_sqlColumns %>% map(function(ls) {
    tmp.df = ls %>% function.list_TABLE_NAME.sqlFetch_sqlColumns.df_COLUMN_TYPE
    tmp.df$exists = T
    left_join(
        solution.list_TABLE_NAME.sqlFetch_sqlColumns.COLUMN_TYPE_df
        , tmp.df
        , by = c("tbl", "COLUMN_NAME", "TYPE_NAME", "TYPE_NAME")
    )
})

students.list_TABLE_NAME.sqlFetch_sqlColumns.df_nrow_FK = students.list_TABLE_NAME.sqlFetch_sqlColumns %>% map_df(function(ls_ls) {
    out = ls_ls[c("tblBeach", "tblBeachUseRequest", "tblPermittee", "tblProgramCode")] %>% map_dbl(function(ls) {
        if (is.null(ls$sqlFetch)) {
            NA
        } else {
            ls$sqlFetch %>% nrow
        }
    })
    out = c(
        out
        , !any(ls_ls$tblBeachUseRequest$sqlFetch$BeachID %in% ls_ls$tblBeach$sqlFetch$BeachID)
        , !any(ls_ls$tblBeachUseRequest$sqlFetch$PermitteeID %in% ls_ls$tblPermittee$sqlFetch$PermitteeID)
    )
    out
})


students.list_TABLE_NAME.sqlFetch_sqlColumns.df_COLUMN_TYPE_nrow_FK = bind_rows(
    bind_cols(
        function.list_TABLE_NAME.sqlFetch_sqlColumns.df_COLUMN_TYPE(solution.list_TABLE_NAME.sqlFetch_sqlColumns)
        , map_df(students.list_TABLE_NAME.sqlFetch_sqlColumns.df_COLUMN_TYPE, function(df) as.numeric(df$exist))
    )
    , mutate(students.list_TABLE_NAME.sqlFetch_sqlColumns.df_nrow_FK, tbl = c(paste0(c("tblBeach", "tblBeachUseRequest", "tblPermittee", "tblProgramCode"), ".nrow"), "BeachID.FK", "PermitteeID.FK") )
) 

openxlsx::write.xlsx(students.list_TABLE_NAME.sqlFetch_sqlColumns.df_COLUMN_TYPE_nrow_FK, file = "students.list_TABLE_NAME.sqlFetch_sqlColumns.df_COLUMN_TYPE_nrow_FK.xlsx", asTable = T)
openxlsx::openXL("students.list_TABLE_NAME.sqlFetch_sqlColumns.df_COLUMN_TYPE_nrow_FK.xlsx")

