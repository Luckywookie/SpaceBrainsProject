using AutoMapper;
using BusinessLogic.DTO.Account;
using BusinessLogic.Services.Base;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using WebAI.Models.Account;

namespace WebAI.Controllers
{
    [Authorize]
    public class UserController : Controller
    {
        IUserService _userService = null;
        IMapper _mapper = null;
        
        public UserController(IUserService userService, IMapper mapper)
        {
            _userService = userService;
            _mapper = mapper;
        }


        public ActionResult UsersList(int id)
        {
            ViewBag.RoleId = id;
            return View(GetUsers());
        }

        IEnumerable<UserViewModel> GetUsers()
        {
            return _mapper.Map<IEnumerable<UserDTO>, IEnumerable<UserViewModel>>(_userService.GetUsers());
        }
    }
}