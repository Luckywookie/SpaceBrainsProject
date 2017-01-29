using AutoMapper;
using BusinessLogic.DTO;
using BusinessLogic.Services.Base;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using WebAI.Models;

namespace WebAI.Controllers
{
    public class CrawlerStatsController : Controller
    {

        ICrawlerStatsService crawlerStatsService = null;
        ISiteService siteService = null;
        public CrawlerStatsController(ICrawlerStatsService crawlerStatsService, ISiteService siteService)
        {
            this.crawlerStatsService = crawlerStatsService;
            this.siteService = siteService;
        }
        
        public IEnumerable<CrawlerStatsViewModel> GetCrawlerStats()
        {

            var stat = crawlerStatsService.GetCrawlerStats();
            Mapper.Initialize(cfg => cfg.CreateMap<CrawlerStatsDTO, CrawlerStatsViewModel>());
            return Mapper.Map<IEnumerable<CrawlerStatsDTO>, IEnumerable<CrawlerStatsViewModel>>(stat);
        }

        public ActionResult Index()
        {
            return View(GetCrawlerStats());
        }


    }
}