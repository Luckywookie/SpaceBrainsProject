using BusinessLogic.Services.Base;
using DataAccess.Repositories.Base;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using WebAI.Models;

namespace WebAI.Controllers
{
    public class HomeController : Controller
    {
        
        public HomeController()
        {
           
        }

         public ActionResult Index()
        {
            return View();
        }


    }
}