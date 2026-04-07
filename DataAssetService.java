package com.hsbc.ce.gdt.refdata.service;

import com.hsbc.ce.gdt.refdata.dto.FilterDto;
import com.hsbc.gdt.refdata.caching.entity.Page;
import com.hsbc.gdt.refdata.caching.entity.Route;
import org.json.simple.JSONObject;
import org.springframework.data.domain.PageRequest;

import java.util.List;
import java.util.Map;

/**
* @author ravidas.ghodse
*/
public interface DataAssetService {

    /***
     * Method to fetch all the records for requested Data Asset
     *
     * @param route    Data Asset Id 
     * @param pageRequest
     * @param filters
     * @return Page<JSONObject>
     */
    Page<JSONObject> getData(Route route, PageRequest pageRequest, Map<String, String> filters);

    Page<JSONObject> getSearchData(Route route, PageRequest pageRequest, Map<String, String> filters);

    Page<JSONObject> getDataWithNestedParams(Route route, PageRequest pageRequest, Map<String, String> filterMap, List<FilterDto> filters);

    Page<JSONObject> getDataForMDM(Route route, PageRequest pageRequest, Map<String, String> filterMap);
}