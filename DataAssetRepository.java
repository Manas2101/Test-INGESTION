package com.hsbc.ce.gdt.refdata.repository;

import com.hsbc.ce.gdt.refdata.dto.FilterDto;
import com.hsbc.gdt.refdata.caching.entity.Page;
import com.hsbc.gdt.refdata.caching.entity.Route;
import org.json.simple.JSONObject;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;

import java.util.List;
import java.util.Map;

/**
* @author ravidas.ghodse
*/
public interface DataAssetRepository {

    /**
     * fetch all data asset data in pages.
     * @param tableName
     * @param filterParamsMap
     * @param page
     * @return
     */
    Page<JSONObject> findAll(String tableName, Map<String, String> filterParamsMap, Pageable page);

    long count(String tableName);

    Page<JSONObject> findAllCoreBankingDetails(String assetName, Map<String, String> filters, PageRequest pageRequest, List<FilterDto> filterDto);

    Page<JSONObject> findMdmDetails(Route route, Map<String, String> filterMap, Pageable pageRequest);
}