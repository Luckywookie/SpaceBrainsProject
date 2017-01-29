using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BusinessLogic.DTO;
using BusinessLogic.Services.Base;
using DataAccess.Repositories.Base;

namespace BusinessLogic.Services
{
    public class CrawlerStatsService :
        Base.ICrawlerStatsService
    {
        ISiteService svc;
        ISiteRepository siteReposytory = null;
        public CrawlerStatsService(ISiteRepository siteReposytory)
        {
            this.siteReposytory = siteReposytory;
        }
        public IEnumerable<CrawlerStatsDTO> GetCrawlerStats()
        {
            List<CrawlerStatsDTO> stats = new List<CrawlerStatsDTO>();
            svc = new SiteService(siteReposytory);
            var sites = svc.GetSites();
            foreach (SiteDTO s in sites)
            {
                CrawlerStatsDTO statsobj = new CrawlerStatsDTO();
                IEnumerable<PageDTO> pages = svc.GetAllPagesForSite(s.Id);
                int countAll = 0;
                int countVisited = 0;
                int countnotVisited = 0;
                foreach (PageDTO p in pages)
                {
                    countAll++;
                    if (p.LastScanDate == null)
                    {
                        countnotVisited++;
                    }
                    else
                    {
                        countVisited++;
                    }
                }
                statsobj.Site = s.Name;
                statsobj.SiteId = s.Id;
                statsobj.CountAllLinks = countAll;
                statsobj.CountNotVisitedLinks = countnotVisited;
                statsobj.CountVisitedLinks = countVisited;
              stats.Add(statsobj);
            }
            
            return stats;
        }
    }
}
