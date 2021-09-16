package com.example.wuzzuf_data_analysis;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StreamUtils;
import org.springframework.web.bind.annotation.*;

import javax.imageio.ImageIO;
import javax.servlet.http.HttpServletResponse;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;


@RestController
@RequestMapping(value="/wuzzufDataAnalysis")
public class WuzzufController {

    @Autowired
    WuzzufService service;


    @GetMapping("/show{num}")
    public List<String> someData(@PathVariable int num){

        return  service.getData(num);
    }

    @GetMapping(value="/summary")
    public List<String> summary(){
        return  service.getSummary();
    }

    @GetMapping(value="/structure")
    public String structure(){
        return  service.getStructure();
    }

    @GetMapping("/clean")
    public List<String> clean(){
        return service.clean();
    }

    @RequestMapping(value = "/demandCompany")
    public List<String> demandingCompanies() throws IOException {
        return service.DemandingCompanies();
    }

    @RequestMapping(value = "/demandCompanyChart", method = RequestMethod.GET, produces = MediaType.IMAGE_PNG_VALUE)
    public void demandingCompaniesChart (HttpServletResponse response) throws IOException {
        ClassPathResource imgFile = new ClassPathResource("charts/company.png");
        StreamUtils.copy(imgFile.getInputStream(), response.getOutputStream());
    }

    @RequestMapping(value = "/mostPopJobs")
    public List<String> mostPopJobs()  {
        return service.mostPopJobs();
    }

    @RequestMapping(value = "/mostPopJobsChart", method = RequestMethod.GET, produces = MediaType.IMAGE_PNG_VALUE)
    public void mostPopJobsChart (HttpServletResponse response) throws IOException {
        ClassPathResource imgFile = new ClassPathResource("charts/Most-Popular-Job-Titles.png");
        StreamUtils.copy(imgFile.getInputStream(), response.getOutputStream());
    }

    @RequestMapping(value = "/mostPopAreas")
    public List<String> mostPopAreas()  {
        return service.mostPopArea();
    }

    @RequestMapping(value = "/mostPopAreasChart", method = RequestMethod.GET, produces = MediaType.IMAGE_PNG_VALUE)
    public void mostPopAreasChart (HttpServletResponse response) throws IOException {
        ClassPathResource imgFile = new ClassPathResource("charts/Most-Popular-Areas.png");
        StreamUtils.copy(imgFile.getInputStream(), response.getOutputStream());
    }

    @RequestMapping(value="/orderSkills")
    public List<Map.Entry> orderSkills(){
        return service.getSkills();
    }


    @RequestMapping(value = "/factorizeYearsExp")
    public List<String> factorizeYearsExp()  {
        return service.factorizeYearsExp();
    }

    @RequestMapping(value="/kMeans")
    public void k_means(){}

}
