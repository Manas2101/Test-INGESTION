@Parameter(name = "rdhLastIngestionTimestamp", description = "Last successful reconciliation timestamp (format: yyyy-MM-dd). Returns only records ingested after this timestamp. Applicable only for CORE_BANKING_COMMON_DATA_MODEL_REFERENCE_CODES data asset.", in = ParameterIn.QUERY, schema = @Schema(type = "string"), example = "2025-12-31")





import java.util.HashMap;


Map<String, String> requestParamsForValidation = new HashMap<>(requestParamsMap);
requestParamsForValidation.remove("rdhLastIngestionTimestamp");
Map<String, String> filterMap = ValidationUtil.getFilterParams(filters, requestParamsForValidation);




String rdhLastIngestionTimestamp = requestParamsMap.get("rdhLastIngestionTimestamp");
if (route.getAssetName().equals(Constants.CORE_BANKING_COMMON_DATA_MODEL_REFERENCE_CODES)) {
    if (rdhLastIngestionTimestamp != null && !rdhLastIngestionTimestamp.isEmpty()) {
        if (!rdhLastIngestionTimestamp.matches("^\\d{4}-\\d{2}-\\d{2}$")) {
            throw new InvalidRequestParamException("rdhLastIngestionTimestamp should be in yyyy-MM-dd format");
        }
        log.info("Filtering records ingested after timestamp: {}", rdhLastIngestionTimestamp);
        filterMap.put("rdhLastIngestionTimestamp", rdhLastIngestionTimestamp);
    }
    data = dataAssetService.getDataWithNestedParams(route, pageRequest, filterMap, filters);
}



import org.springframework.util.StringUtils;


Map<String, String> workingFilterParams = new HashMap<>(filterParamsMap);
String rdhLastIngestionTimestamp = workingFilterParams.remove("rdhLastIngestionTimestamp");


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

boolean hasRootWhereCondition = !whereClause.isBlank() && !"WHERE".equalsIgnoreCase(whereClause.trim());
String selectSql = buildSelectSql(tableName, whereClause, nestedWhereClause, !filterParamsSecondary.isEmpty(), hasRootWhereCondition);
String countSql = buildCountSql(tableName, whereClause, nestedCondition, nestedWhereClause, !filterParamsSecondary.isEmpty(), hasRootWhereCondition);


if (rdhLastIngestionTimestamp != null && !rdhLastIngestionTimestamp.isEmpty()) {
    allFilterParams.put("rdhLastIngestionTimestamp", rdhLastIngestionTimestamp);
}



} catch (Exception e) {
    log.error("xMatter Alert Trigger - RefdataGenericAPI Exception occurred in DataAssetRepositoryImpl - findAllCoreBankingDetails(): Query failed!: ", e);
    String rootCauseMsg = e.getCause() != null ? e.getCause().getMessage() : e.getMessage();
    String errorMsg = (rootCauseMsg == null || rootCauseMsg.isBlank())
        ? FAILED_TO_GET_REQUESTED_DATA
        : FAILED_TO_GET_REQUESTED_DATA + " - " + rootCauseMsg;
    throw new InvalidRequestParamException(errorMsg);
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
        return selectQueryForNested() + nestedWhereClause + whereClauseForNestedQuery();
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