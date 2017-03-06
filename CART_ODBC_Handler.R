require(RODBC)
require(sqldf)
require(tcltk)
require(dplyr)
require(readr)
require(lubridate)
odbcConnection <- function()
{
  login_df <- read.csv("../odbc_login.csv", stringsAsFactors = F, header = TRUE)
  conn <- odbcConnect(login_df$odbc_profile[1], uid=login_df$login_id[1], pwd=login_df$password[1], believeNRows=FALSE)
  # dbDisconnect(conn)
  conn
}


categoryToPscLookup <- function(categoryName)
{
  psc_lookup_table <- read.csv("./pscTransTable.csv", header = TRUE, stringsAsFactors = FALSE)
  names(psc_lookup_table)<-c("description", "psc", "catcode", "level1", "subcatcode", "level2")
  psc_lookup_table$level1 <- trimws(psc_lookup_table$level1, "both")
  psc_vector <- psc_lookup_table[which(psc_lookup_table$level1==categoryName), 2]
  psc_vector  
}

subcategoryToPscLookup <- function(subcategoryName)
{
  # transactionDfSubSet <- transactionDfSuperSet[0,]
  psc_lookup_table <- read.csv("pscTransTable.csv", header = TRUE, stringsAsFactors = FALSE)
  names(psc_lookup_table)<-c("description", "psc", "catcode", "level1", "subcatcode", "level2")
  psc_vector <- psc_lookup_table[which(psc_lookup_table$level2==subcategoryName), 2]
  psc_vector  
}



fpds_query <- function(date_start, date_end, psc_list)
{
  
  selectBody<- c("Select  a_aid_acontid_piid,
                 ag_contract_piid,
                 agency_code,
                 ag_name, 
                 bureau_code, 
                 bureau_name,
                 co_name,
                 cd_contactiontype,
                 cd_descofcontreq,
                 ultimatecompletiondate,
                 market.fpdsxi_ng_data.effectivedate,
                 funding_agency_name,
                 mod_num,
                 naics_code,
                 naics_name,
                 pscinfo_principalnaicscode,
                 market.fpdsxi_ng_data.obligatedamount,
                 primary_contract_piid,
                 pscinfo_productorservicecode,
                 prod_or_serv_code,
                 prod_or_serv_code_desc,
                 refidvid_piid,
                 refidvid_modnumber,
                 market.fpdsxi_ng_data.signeddate,
                 vend_contoffbussizedeterm,
                 vend_dunsnumber,
                 vend_vendorname,
                 whocanuse,
                 pop_zipcode,
                 ppop_city_desc,
                 ppop_country_desc,
                 ppop_county_desc,
                 ppop_state,
                 transinfo_createddate,
                 transinfo_lastmodifieddate
                 FROM market.fpdsxi_ng_data     
                 INNER JOIN market.fpdsxi_idvs on market.fpdsxi_idvs.idv_key = market.fpdsxi_ng_data.idv_key
                 WHERE 
                 convert(varchar(25),dateformat(substring(market.fpdsxi_ng_data.signeddate,1,10),'yyyy-mm-dd'))  >=  ")
  startDateAnd <- c(" and")
  
  endDateSql  <-  c("convert(varchar(25),dateformat(substring(market.fpdsxi_ng_data.signeddate,1,10),'yyyy-mm-dd'))  <=  ") 
  
  sqlQueryP3 <- c("and  \n \t\t\t\t\t\t\t\t prod_or_serv_code in(")
  
  psc_count <- length(psc_list)
  for(i in 1:psc_count)
  {
    sqlQueryP3 <- paste(sqlQueryP3,"'",psc_list[i],"'" ,sep = "")
    
    if(i< psc_count)sqlQueryP3 <- paste(sqlQueryP3,",", sep = "")
  }
  
  sqlQueryP3 <- paste(sqlQueryP3,")", sep = "")
  
  
  thisQuery <- sprintf("%s '%s' %s %s '%s' %s", selectBody, date_start, startDateAnd, endDateSql, date_end , sqlQueryP3 )
  thisQuery
  
}

get_one_cat <-function(category_name)
{
  conn <- odbcConnection()
  composite <- sqlQuery(conn, fpds_query("2011-10-01", "2016-09-30", categoryToPscLookup(category_name)))
  odbcClose(conn) 
  composite
}


AllCatsDf <- function(start_date, end_date)
{
  
  #Must apply partial fetching for size
  conn <- odbcConnection()
  composite <- sqlQuery(conn, fpds_query(start_date, end_date, categoryToPscLookup("Facilities & Construction")), stringsAsFactors = FALSE)
  odbcClose(conn)
  conn <- odbcConnection()
  composite <- rbind(composite, sqlQuery(conn, fpds_query(start_date, end_date, categoryToPscLookup("Human Capital"))), stringsAsFactors = FALSE)
  odbcClose(conn)
  conn <- odbcConnection()
  composite <- rbind(composite, sqlQuery(conn, fpds_query(start_date, end_date, categoryToPscLookup("Industrial Products & Services"))), stringsAsFactors = FALSE)
  odbcClose(conn)
  conn <- odbcConnection()
  composite <- rbind(composite, sqlQuery(conn, fpds_query(start_date, end_date, categoryToPscLookup("IT"))), stringsAsFactors = FALSE)
  odbcClose(conn)
  conn <- odbcConnection()
  composite <- rbind(composite, sqlQuery(conn, fpds_query(start_date, end_date, categoryToPscLookup("Medical"))), stringsAsFactors = FALSE)
  odbcClose(conn)
  conn <- odbcConnection()
  composite <- rbind(composite, sqlQuery(conn, fpds_query(start_date, end_date, categoryToPscLookup("Office Management"))), stringsAsFactors = FALSE)
  odbcClose(conn)
  conn <- odbcConnection()
  composite <- rbind(composite, sqlQuery(conn, fpds_query(start_date, end_date, categoryToPscLookup("Professional Services"))), stringsAsFactors = FALSE)
  odbcClose(conn)
  conn <- odbcConnection()
  composite <- rbind(composite, sqlQuery(conn, fpds_query(start_date, end_date, categoryToPscLookup("Security and Protection"))), stringsAsFactors = FALSE)
  odbcClose(conn)
  conn <- odbcConnection()
  composite <- rbind(composite, sqlQuery(conn, fpds_query(start_date, end_date, categoryToPscLookup("Transportation and Logistics Services"))), stringsAsFactors = FALSE)
  odbcClose(conn)
  conn <- odbcConnection()
  composite <- rbind(composite, sqlQuery(conn, fpds_query(start_date, end_date, categoryToPscLookup("Travel & Lodging"))), stringsAsFactors = FALSE)
  odbcClose(conn)
  composite
}






