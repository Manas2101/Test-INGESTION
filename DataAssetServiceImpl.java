package com.hsbc.ce.gdt.refdata.service;


import com.hsbc.ce.gdt.refdata.constant.Constants;
import com.hsbc.ce.gdt.refdata.dto.FilterDto;
import com.hsbc.ce.gdt.refdata.repository.DataAssetRepository;
import com.hsbc.ce.gdt.refdata.repository.DataAssetRepositoryWithLike;
import com.hsbc.gdt.refdata.caching.entity.Page;
import com.hsbc.gdt.refdata.caching.entity.Route;
import com.hsbc.gdt.refdata.caching.repository.CacheDataAssetRepository;
import lombok.extern.slf4j.Slf4j;
import org.json.simple.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Map;

import static com.hsbc.ce.gdt.refdata.constant.Constants.VERSION;


/**
* @author ravidas.ghodse
*/
@Service
@Slf4j
public class DataAssetServiceImpl implements DataAssetService {

    private final DataAssetRepository dataAssetRepository;

    private final DataAssetRepositoryWithLike dataAssetRepositoryWithLike;
    private final CacheDataAssetRepository cacheDataAssetRepository;

    @Autowired
    public DataAssetServiceImpl(DataAssetRepository dataAssetRepository, DataAssetRepositoryWithLike dataAssetRepositoryWithLike, CacheDataAssetRepository cacheDataAssetRepository) {
        this.dataAssetRepository = dataAssetRepository;
        this.dataAssetRepositoryWithLike = dataAssetRepositoryWithLike;
        this.cacheDataAssetRepository = cacheDataAssetRepository;
    }


    @Override
    public Page<JSONObject> getData(Route route, PageRequest pageRequest,
                                    Map<String, String> filters) {
        filters.remove(Constants.GUID);
        if (route.getAssetName().equals(Constants.DIGITAL_DISCLOSURE_DATA_ASSET)) {
            log.info("RefdataGenericAPI - inside DataAssetServiceImpl - getData() for DataAsset: " + route.getAssetName());
            try {
                return cacheDataAssetRepository.findCustomerCareNeedsDescription(route.getAssetName(), filters, pageRequest);
            } catch (Exception e) {
                log.error("xMatter Alert Trigger - RefdataGenericAPI Exception: []", e);
                return dataAssetRepository.findAll(route.getAssetName(), filters, pageRequest);
            }
        } else {
            return dataAssetRepository.findAll(route.getAssetName(), filters, pageRequest);
        }
    }

    @Override
    public Page<JSONObject> getSearchData(Route route, PageRequest pageRequest, Map<String, String> filters) {
        filters.remove(Constants.GUID);
        return dataAssetRepositoryWithLike.findAllWithLike(route.getAssetName(), filters, pageRequest);
    }
   

    @Override
    public Page<JSONObject> getDataWithNestedParams(Route route, PageRequest pageRequest, Map<String, String> filterMap, List<FilterDto> filters) {
        filterMap.remove(Constants.GUID);
        return dataAssetRepository.findAllCoreBankingDetails(route.getAssetName(), filterMap, pageRequest, filters);
    }

   
    @Override
    public Page<JSONObject> getDataForMDM(Route route, PageRequest pageRequest, Map<String, String> filterMap) {
        filterMap.remove(Constants.GUID);
        if (filterMap.containsKey(VERSION)) {
            try {
                String oldVersion = filterMap.get(VERSION);
                if (oldVersion != null) {
                    BigDecimal formattedVersion = new BigDecimal(oldVersion.trim()).setScale(1, RoundingMode.HALF_UP);
                    filterMap.put(VERSION, formattedVersion.toString());
                }
            } catch (NumberFormatException e) {
                log.error("Invalid version format in filterMap: {}", filterMap.get(VERSION), e);
            }
        }
        return dataAssetRepository.findMdmDetails(route, filterMap, pageRequest);
    }

}