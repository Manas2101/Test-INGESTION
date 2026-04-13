package com.hsbc.ce.gdt.refdata.repository;

 

import com.hsbc.ce.gdt.refdata.constant.Constants;

import com.hsbc.ce.gdt.refdata.dto.FilterDto;

import com.hsbc.ce.gdt.refdata.exception.InvalidRequestParamException;

import com.hsbc.ce.gdt.refdata.exception.RecordNotFoundException;

import com.hsbc.gdt.refdata.caching.entity.Page;

import com.hsbc.gdt.refdata.caching.entity.Route;

import lombok.extern.slf4j.Slf4j;

import org.json.simple.JSONObject;

import org.json.simple.parser.JSONParser;

import org.json.simple.parser.ParseException;

import org.springframework.beans.factory.annotation.Autowired;

import org.springframework.data.domain.PageRequest;

import org.springframework.data.domain.Pageable;

import org.springframework.data.domain.Sort.Order;

import org.springframework.jdbc.CannotGetJdbcConnectionException;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import org.springframework.stereotype.Repository;

import org.springframework.util.StringUtils;

 

import java.sql.ResultSet;

import java.sql.SQLException;

import java.util.*;

 

import static com.hsbc.ce.gdt.refdata.constant.Constants.*;

 

/**

* @author ravidas.ghodse

*/

@Repository

@Slf4j

public class DataAssetRepositoryImpl implements DataAssetRepository {

 

               private final NamedParameterJdbcTemplate jdbcTemplate;

 

               @Autowired

               public DataAssetRepositoryImpl(NamedParameterJdbcTemplate jdbcTemplate) {

                              this.jdbcTemplate = jdbcTemplate;

               }

 

               /***

               * Method to fetch all the records for requested Data Asset

               */

               @Override

               public Page<JSONObject> findAll(String tableName, Map<String, String> filterParamsMap, Pageable page) {

                              log.info("RefdataGenericAPI - inside DataAssetRepositoryImpl - findAll() for DataAsset: "+ tableName);

                              long count;

                              // Validate or whitelist the tableName to prevent SQL injection

                              String validatedTableName= validatedTableName(tableName);

                              if(validatedTableName == null){

                                             throw new InvalidRequestParamException("Invalid table name provided.");

                              }

                              String whereClause = getWhereClause(filterParamsMap);

                              String selectSql = "SELECT * FROM " + validatedTableName + whereClause

                                                            + ORDER_BY_LIMIT_OFFSET;

                              MapSqlParameterSource selectSqlParams = getSelectSqlParams(page, filterParamsMap);

                              try {

                                             count = count(validatedTableName, whereClause, filterParamsMap);

                                             if (count <= 0)

                                                            throw new RecordNotFoundException();

                                             List<String> rows = jdbcTemplate.query(selectSql, selectSqlParams,

                                                                           (ResultSet rs, int rowNum) -> mapDataRow(rs));

                                             if (rows.isEmpty())

                                                            throw new RecordNotFoundException();

                                             List<JSONObject> collect = rows.stream().map(this::convertToJson).toList();

                                             return new Page<>(collect, page, count);

                              } catch (CannotGetJdbcConnectionException e) {

                                             log.error(

                                                                           "xMatter Alert Trigger-RefdataGenericAPI JdbcConnection Exception occurred in DataAssetRepositoryImpl - findAll(): Query failed!: "

                                                                           + e);

                                             throw e;

                              } catch (RecordNotFoundException e) {

                                             log.error(NO_RECORDS_FOUND_FOR_GIVEN_CRITERIA);

                                             throw new RecordNotFoundException();

                              } catch (Exception e) {

                                             log.error(

                                                                           "xMatter Alert Trigger - RefdataGenericAPI Exception occurred in DataAssetRepositoryImpl - findAll(): Query failed!: "

                                                                           + e);

                                             throw new InvalidRequestParamException(FAILED_TO_GET_REQUESTED_DATA);

                              }

               }

 

               private String validatedTableName(String tableName) {

                              // Implement a whitelist or regex validation for table names

                              if(tableName.matches("^[a-zA-Z0-9_]+$")){

                                             return tableName;

                              }else{

                                             return null;

                              }

               }

 

               @Override

               public long count(String tableName) {

                              return count(tableName, "", new HashMap<>());

               }

 

               public Page<JSONObject> findAllCoreBankingDetails(String tableName, Map<String, String> filterParamsMap, PageRequest page, List<FilterDto> filtersDtos) {

                              log.info("RefdataGenericAPI - inside DataAssetRepositoryImpl - findAllCoreBankingDetails() for DataAsset: " + tableName);

               Map<String, String> workingFilterParams = new HashMap<>(filterParamsMap);

               String rdhLastIngestionTimestamp = workingFilterParams.remove("rdhLastIngestionTimestamp");

 

                              //differentiating primary and secondary query params

                              Map<String, String> filterParamsPrimary = extractFilterParams(workingFilterParams, filtersDtos, "primary");

                              Map<String, String> filterParamsSecondary = extractFilterParams(workingFilterParams, filtersDtos, "secondary");

                              String whereClause = "";

                              if(filterParamsPrimary.isEmpty() && filterParamsSecondary.isEmpty()){

                                             whereClause="";

                              }else {

                                             whereClause = !getWhereClause(filterParamsPrimary).isEmpty() ? getWhereClause(filterParamsPrimary) : " WHERE";

                              }

               if (rdhLastIngestionTimestamp != null && !rdhLastIngestionTimestamp.isEmpty()) {

                              String rdhCondition = "left(regexp_replace(coalesce(jsondata->>'rdhLastIngestionTimestamp',''), '[^0-9]', '', 'g'), 8) > replace(:rdhLastIngestionTimestamp, '-', '')";

                              if (whereClause.isEmpty()) {

                                             whereClause = " WHERE " + rdhCondition;

                              } else if ("WHERE".equalsIgnoreCase(whereClause.trim())) {

                                             whereClause = whereClause + " " + rdhCondition;

                              } else {

                                             whereClause = whereClause + AND + rdhCondition;

                              }

               }

                              String nestedWhereClause = getWhereClauseSecondary(filterParamsSecondary);

                              String nestedCondition = " EXISTS (SELECT 1 FROM jsonb_array_elements(jsondata->'coreBankingSourceSystemReferenceData') AS elem";

                              boolean hasRootWhereCondition = !whereClause.isBlank() && !"WHERE".equalsIgnoreCase(whereClause.trim());

                              String selectSql = buildSelectSql(tableName, whereClause, nestedWhereClause, !filterParamsSecondary.isEmpty(), hasRootWhereCondition);

                              String selectSqlWithPagination= selectSql + ORDER_BY_LIMIT_OFFSET;

                              String countSql = buildCountSql(tableName, whereClause, nestedCondition, nestedWhereClause, !filterParamsSecondary.isEmpty(), hasRootWhereCondition);

 

                              Map<String, String> allFilterParams = new HashMap<>();

                              allFilterParams.putAll(filterParamsPrimary);

                              allFilterParams.putAll(filterParamsSecondary);

                              if (rdhLastIngestionTimestamp != null && !rdhLastIngestionTimestamp.isEmpty()) {

                                             allFilterParams.put("rdhLastIngestionTimestamp", rdhLastIngestionTimestamp);

                              }

                              MapSqlParameterSource selectSqlParams = getSelectSqlParams(page, allFilterParams);

 

                              try {

                                             long count = jdbcTemplate.queryForObject(countSql, selectSqlParams, Long.class);

                                             if (count <= 0) throw new RecordNotFoundException();

 

                                             List<String> rows = jdbcTemplate.query(selectSqlWithPagination, selectSqlParams, (ResultSet rs, int rowNum) -> mapDataRow(rs));

                                             if (rows.isEmpty()) throw new RecordNotFoundException();

 

                                             List<JSONObject> collect = rows.stream().map(this::convertToJson).toList();

                                             return new Page<>(collect, page, count);

                              } catch (CannotGetJdbcConnectionException e) {

                                             log.error("xMatter Alert Trigger-RefdataGenericAPI JdbcConnection Exception occurred in DataAssetRepositoryImpl - findAllCoreBankingDetails(): Query failed!: ", e);

                                             throw e;

                              } catch (RecordNotFoundException e) {

                                             log.error(NO_RECORDS_FOUND_FOR_GIVEN_CRITERIA);

                                             throw e;

                              } catch (Exception e) {

                                             log.error("xMatter Alert Trigger - RefdataGenericAPI Exception occurred in DataAssetRepositoryImpl - findAllCoreBankingDetails(): Query failed!: ", e);

                                             String rootCauseMsg = e.getCause() != null ? e.getCause().getMessage() : e.getMessage();

                                             String errorMsg = (rootCauseMsg == null || rootCauseMsg.isBlank())

                                                            ? FAILED_TO_GET_REQUESTED_DATA

                                                            : FAILED_TO_GET_REQUESTED_DATA + " - " + rootCauseMsg;

                                             throw new InvalidRequestParamException(errorMsg);

                              }

               }

 

               @Override

               public Page<JSONObject> findMdmDetails(Route route, Map<String, String> filterParamsMap, Pageable page) {

                              log.info("RefdataGenericAPI - inside DataAssetRepositoryImpl - findAll() for DataAsset: {}", route.getAssetName());

 

                              String tableName = route.getAssetName();

                              String routeName = route.getRouteId();

                              String validatedTableName = validatedTableName(tableName);

 

                              if (validatedTableName == null) {

                                             throw new InvalidRequestParamException("Invalid table name provided.");

                              }

 

                              String whereClause = getWhereClause(filterParamsMap);

                              String selectSql = buildSelectSqlForMDM(tableName, whereClause, filterParamsMap, routeName);

                              String selectCountSql = buildSelectSqlCountForMDM(tableName, whereClause, filterParamsMap, routeName);

                              MapSqlParameterSource selectSqlParams = getSelectSqlParams(page, filterParamsMap);

 

                              try {

                                             long count = getCount(selectCountSql, selectSqlParams);

                                             if (count <= 0) {

                                                            throw new RecordNotFoundException();

                                             }

                                             List<Map<String, Object>> rows = getQueryResults(selectSql, selectSqlParams);

                                             if (rows.isEmpty()) {

                                                            throw new RecordNotFoundException();

                                             }

                                             return buildPageResult(rows, page, count);

                              } catch (CannotGetJdbcConnectionException e) {

                                             log.error("JdbcConnection Exception in findMdmDetails(): Query failed!", e);

                                             throw e;

                              } catch (RecordNotFoundException e) {

                                             log.error(NO_RECORDS_FOUND_FOR_GIVEN_CRITERIA);

                                             throw e;

                              } catch (Exception e) {

                                             log.error("Exception in findMdmDetails(): Query failed!", e);

                                             throw new InvalidRequestParamException(FAILED_TO_GET_REQUESTED_DATA);

                              }

               }

 

               private long getCount(String selectCountSql, MapSqlParameterSource selectSqlParams) {

                              Long count=  jdbcTemplate.queryForObject(selectCountSql, selectSqlParams, Long.class);

                              return count != null ? count :0;

               }

 

               private List<Map<String, Object>> getQueryResults(String selectSql, MapSqlParameterSource selectSqlParams) {

                              return jdbcTemplate.queryForList(selectSql, selectSqlParams);

               }

 

               private Page<JSONObject> buildPageResult(List<Map<String, Object>> rows, Pageable page, long count) throws ParseException {

                              JSONObject jsonObject = new JSONObject();

                              if(rows != null && rows.get(0) != null && rows.get(0).get(METADATA)!=null) {

 

                                             jsonObject.put(METADATA, new JSONParser().parse(rows.get(0).get(METADATA).toString()));

                              }

 

                              List<JSONObject> jsonObjects = new ArrayList<>();

                              if(rows != null && rows.get(0) != null && rows.get(0).get("attributedetails")!=null) {

 

                                             for (Map<String, Object> row : rows) {

                                                            jsonObjects.add((JSONObject) new JSONParser().parse(row.get("attributedetails").toString()));

                                             }

                                             jsonObject.put("attributeDetails", jsonObjects);

                              }

 

                              List<JSONObject> finalData = new ArrayList<>();

                              finalData.add(jsonObject);

 

                              return new Page<>(finalData, page, count);

               }

 

               protected String buildSelectSqlForMDM(String tableName, String whereClause, Map<String, String> filterParamsMap, String routeName) {

                              StringBuilder query = new StringBuilder();

                              if (filterParamsMap.containsKey(VERSION) || filterParamsMap.containsKey(MDM_STATUS)) {

                                             whereClause = whereClause.replaceAll("WHERE jsondata->>'version' = :version|AND jsondata->>'version' = :version", "");

                                             whereClause = whereClause.replaceAll("WHERE jsondata->>'mdm_status' = :mdm_status|AND jsondata->>'mdm_status' = :mdm_status", "");

 

                                             query.append("SELECT a.jsondata AS attributeDetails, b.version_details AS metadata ")

                                                                           .append(RDH_API_POD2).append(tableName).append(" a JOIN (SELECT effective_date, version_details FROM rdh_api_pod2.data_asset_version WHERE ");

 

                                             if (filterParamsMap.get(VERSION) != null) {

                                                            query.append("Version = '").append(filterParamsMap.get(VERSION)).append("' ");

                                             }

                                             if (filterParamsMap.containsKey(MDM_STATUS)) {

                                                            if (filterParamsMap.get(VERSION) != null) {

                                                                           query.append("AND ");

                                                            }

                                                            query.append("mdm_status = '").append(filterParamsMap.get(MDM_STATUS)).append("' ");

                                             }

 

                                             query.append("AND REPLACE(LOWER(Data_Asset_Name), '_', '-') = '").append(routeName).append("') b ")

                                                                           .append("ON a.effectivedate  = b.effective_date ")

                                                                           .append("AND a.expirationdate  >= b.effective_date ");

                              } else {

                                             query.append("SELECT a.jsondata AS attributeDetails, b.version_details AS metadata ")

                                                                           .append(RDH_API_POD2).append(tableName).append(" a JOIN (SELECT effective_date, version_details FROM rdh_api_pod2.data_asset_version ")

                                                                           .append("WHERE REPLACE(LOWER(Data_Asset_Name), '_', '-') = '").append(routeName).append("' ")

                                                                           .append("AND effective_date = (SELECT MAX(effective_date) FROM rdh_api_pod2.data_asset_version ")

                                                                           .append("WHERE REPLACE(LOWER(Data_Asset_Name), '_', '-') = '").append(routeName).append("')) b ")

                                                                           .append("ON a.effectivedate  = b.effective_date ")

                                                                           .append("AND a.expirationdate  >= b.effective_date ");

                              }

 

                              query.append(whereClause).append(ORDER_BY_LIMIT_OFFSET);

                              return query.toString();

               }

 

               protected String buildSelectSqlCountForMDM(String tableName, String whereClause, Map<String, String> filterParamsMap, String routeName) {

                              StringBuilder query = new StringBuilder("select count(*) from (select 1 " + RDH_API_POD2 + tableName + " a join (select effective_date, version_details from rdh_api_pod2.data_asset_version ");

 

                              if (filterParamsMap.containsKey(VERSION) || filterParamsMap.containsKey(MDM_STATUS)) {

                                             whereClause = whereClause.replaceAll("WHERE jsondata->>'version' = :version|AND jsondata->>'version' = :version", "")

                                                                           .replaceAll("WHERE jsondata->>'mdm_status' = :mdm_status|AND jsondata->>'mdm_status' = :mdm_status", "");

 

                                             query.append("where ");

                                             if (filterParamsMap.get(VERSION) != null) {

                                                            query.append("Version = '").append(filterParamsMap.get(VERSION)).append("' ");

                                             }

                                             if (filterParamsMap.containsKey(MDM_STATUS)) {

                                                            if (filterParamsMap.get(VERSION) != null) {

                                                                           query.append("and ");

                                                            }

                                                            query.append("mdm_status = '").append(filterParamsMap.get(MDM_STATUS)).append("' ");

                                             }

                                             query.append("and replace(lower(Data_Asset_Name), '_', '-') = '").append(routeName).append("') b ");

                              } else {

                                             query.append("where replace(lower(Data_Asset_Name), '_', '-') = '").append(routeName).append("' ")

                                                                           .append("and effective_date = (select max(effective_date) from rdh_api_pod2.data_asset_version ")

                                                                           .append("where replace(lower(Data_Asset_Name), '_', '-') = '").append(routeName).append("')) b ");

                              }

                              query.append("on a.effectivedate  = b.effective_date ")

                                                            .append("and a.expirationdate >= b.effective_date ")

                                                            .append(whereClause)

                                                            .append(" )paged_data");

 

                              return query.toString();

               }

 

 

 

                              private Map<String, String> extractFilterParams(Map<String, String> filterParamsMap, List<FilterDto> filtersDtos, String attributeType) {

                              Map<String, String> extractedParams = new HashMap<>();

                              filtersDtos.stream()

                                                            .filter(filter -> attributeType.equalsIgnoreCase(filter.getAttributeType()))

                                                            .forEach(filter -> {

                                                                           String jsonAttribute = normalizeFilterKey(filter.getJsonAttribute());

                                                                           String filterName = normalizeFilterKey(filter.getFilterName());

                                                                           String resolvedKey = StringUtils.hasText(jsonAttribute) ? jsonAttribute : filterName;



                                                                           if (!StringUtils.hasText(resolvedKey)) {

                                                                                          return;

                                                                           }



                                                                           if (filterParamsMap.containsKey(resolvedKey)) {

                                                                                          extractedParams.put(resolvedKey, filterParamsMap.get(resolvedKey));

                                                                           } else if (filterParamsMap.containsKey(filterName)) {

                                                                                          extractedParams.put(resolvedKey, filterParamsMap.get(filterName));

                                                                           }

                                                            });

                              return extractedParams;

               }



               private String normalizeFilterKey(String key) {

                              return key == null ? null : key.trim();

               }

 

               private String buildSelectSql(String tableName, String whereClause, String nestedWhereClause, boolean hasSecondary, boolean hasPrimary) {

                              if (hasPrimary && hasSecondary) {

                                             String existsCondition = "EXISTS (SELECT 1 FROM jsonb_array_elements(jsondata->'coreBankingSourceSystemReferenceData') AS elem"

                                                            + nestedWhereClause + " )";

                                             return selectQueryForPrimaryAndNested()

                                                            + nestedWhereClause

                                                            + whereClauseForPrimaryAndNestedQuery()

                                                            + appendCondition(whereClause, existsCondition);

                              } else if (hasSecondary) {

                                             return  selectQueryForNested()+ nestedWhereClause +whereClauseForNestedQuery();

                              } else {

                                             return "SELECT * FROM " + tableName + (whereClause.isEmpty() ? "" : whereClause);

                              }

               }



               private String appendCondition(String whereClause, String condition) {

                              if (whereClause == null || whereClause.isBlank()) {

                                             return " WHERE " + condition;

                              }

                              if ("WHERE".equalsIgnoreCase(whereClause.trim())) {

                                             return whereClause + " " + condition;

                              }

                              return whereClause + AND + condition;

               }

 

               private String buildCountSql(String tableName, String whereClause, String nestedCondition, String nestedWhereClause, boolean hasSecondary, boolean hasPrimary) {

                              if (hasPrimary && hasSecondary) {

                                             return "SELECT count(1) FROM " + tableName + whereClause + AND + nestedCondition + nestedWhereClause + " );";

                              } else if (hasSecondary) {

                                             return  selectCountQueryForNested()+ nestedWhereClause +whereClauseForNestedQuery();

                              } else {

                                             return "SELECT count(1) FROM " + tableName + (whereClause.isEmpty() ? "" : whereClause);

                              }

               }

 

               private String whereClauseForNestedQuery() {

                              return " )" +

                                                            "  ) AS filtered_json " +

                                                            "FROM rdh_api_pod2.core_banking_common_data_model_reference_codes " +

                                                            ")a where a.filtered_json is not null";

               }

 

               private String whereClauseForPrimaryAndNestedQuery() {

                              return " )" +

                                                            "  ) AS jsondata " +

                                                            "FROM rdh_api_pod2.core_banking_common_data_model_reference_codes ";

               }

 

               private String selectQueryForNested() {

                              return "  select id, filtered_json as jsondata from " +

                                                            "(" +

                                                            "SELECT" +

                                                            "  id," +

                                                            "  jsonb_set(" +

                                                            "    jsondata," +

                                                            "    '{coreBankingSourceSystemReferenceData}'," +

                                                            "    (" +

                                                            "      SELECT jsonb_agg(elem)" +

                                                            "      FROM jsonb_array_elements(jsondata->'coreBankingSourceSystemReferenceData') AS elem ";

               }

 

               private String selectQueryForPrimaryAndNested() {

                              return "    SELECT" +

                                                            "  jsonb_set(" +

                                                            "    jsondata," +

                                                            "    '{coreBankingSourceSystemReferenceData}'," +

                                                            "    (" +

                                                            "      SELECT jsonb_agg(elem)" +

                                                            "      FROM jsonb_array_elements(jsondata->'coreBankingSourceSystemReferenceData') AS elem ";

               }

 

               private String selectCountQueryForNested() {

                              return " select count(id) from" +

                                                            "(" +

                                                            "SELECT" +

                                                            "  id," +

                                                            "  jsonb_set(" +

                                                            "    jsondata," +

                                                            "    '{coreBankingSourceSystemReferenceData}'," +

                                                            "    (" +

                                                            "      SELECT jsonb_agg(elem)" +

                                                            "      FROM jsonb_array_elements(jsondata->'coreBankingSourceSystemReferenceData') AS elem ";

               }

 

 

               /***

               * Method to map filter and pagination params to sqlParams

               *

               * @param page            pagination params

               * @param filterParamsMap filter params

               * @return sqlParams

               */

               public MapSqlParameterSource getSelectSqlParams(Pageable page, Map<String, String> filterParamsMap) {

                              MapSqlParameterSource sqlParams = getWhereSqlParams(filterParamsMap);

                              Order order;

                              if (!page.getSort().isEmpty())

                                             order = page.getSort().toList().get(0);

                              else

                                             order = Order.by("id");

 

                              sqlParams.addValue("orderBy", order.getDirection().name());

                              sqlParams.addValue("limit", page.getPageSize());

                              sqlParams.addValue("offset", page.getOffset());

                              return sqlParams;

               }

 

               public MapSqlParameterSource getWhereSqlParams(Map<String, String> filterParamsMap) {

                              MapSqlParameterSource sqlParams = new MapSqlParameterSource();

                              filterParamsMap.forEach(sqlParams::addValue);

                              return sqlParams;

               }

 

               /***

               * Method to generate conditional statement

               *

               * @param filterParamsMap

               * @return String condition

               */

               public String getWhereClause(Map<String, String> filterParamsMap) {

                              StringJoiner where = new StringJoiner(AND, " WHERE ", "").setEmptyValue("");

                              filterParamsMap.remove("guid");

                              filterParamsMap.forEach((key, value) -> {

                                             String normalizedKey = normalizeFilterKey(key);

                                             if (StringUtils.hasText(normalizedKey)) {

                                                            where.add("jsondata->>'" + normalizedKey + "' = :" + normalizedKey);

                                             }

                              });

                              return where.toString();

               }

 

 

               public String getWhereClauseSecondary(Map<String, String> filterParamsMap) {

                              StringJoiner where = new StringJoiner(AND, " WHERE ", "").setEmptyValue("");

                              filterParamsMap.remove("guid");

                              filterParamsMap.forEach((key, value) -> {

                                             String normalizedKey = normalizeFilterKey(key);

                                             if (StringUtils.hasText(normalizedKey)) {

                                                            if ("countryCode".equalsIgnoreCase(normalizedKey) || "sourceSystemName".equalsIgnoreCase(normalizedKey)) {

                                                                           where.add("lower(trim(elem->>'" + normalizedKey + "')) = lower(trim(:" + normalizedKey + "))");

                                                            } else {

                                                                           where.add("elem->>'" + normalizedKey + "' = :" + normalizedKey);

                                                            }

                                             }

                              });

                              return where.toString();

               }

 

               /***

               * Method to fetch the total record count for requested Data Asset

               *

               * @param tableName       Data Asset table name

               * @param whereClause     conditional statement

               * @param filterParamsMap filter params

               * @return long total record count

               */

               private long count(String tableName, String whereClause, Map<String, String> filterParamsMap) {

                              String countSql = "SELECT count(1) from " + tableName + whereClause;

                              MapSqlParameterSource sqlParams = getWhereSqlParams(filterParamsMap);

                              long queryForObject = 0;

                              try {

                                             queryForObject = jdbcTemplate.queryForObject(countSql, sqlParams, Long.class);

                              } catch (CannotGetJdbcConnectionException e) {

                                             log.error(

                                                                           "xMatter Alert Trigger-RefdataGenericAPI JdbcConnection Exception occurred in DataAssetRepositoryImpl - count(): Query failed!: "

                                                                           + e);

                                             throw e;

                              } catch (Exception e) {

                                             log.error(

                                                                           "xMatter Alert Trigger - RefdataGenericAPI Exception occurred in DataAssetRepositoryImpl - count(): Failed to count!: "

                                                                           + e);

                              }

                              return queryForObject;

               }

 

               private String mapDataRow(ResultSet rs) throws SQLException {

                              return rs.getString(Constants.JSONDATA);

               }

 

               /***

               * Method to convert String into JSONObject

               *

               * @param strJson

               * @return JSONObject

               */

               public JSONObject convertToJson(String strJson) {

                              JSONParser parser = new JSONParser();

                              try {

                                             return (JSONObject) parser.parse(strJson);

                              } catch (ParseException e) {

                                             log.error(

                                                                           "xMatter Alert Trigger - RefdataGenericAPI Exception occurred in DataAssetRepositoryImpl - convertToJson(): "

                                                                           + e);

                              }

                              return new JSONObject();

               }

 

}

