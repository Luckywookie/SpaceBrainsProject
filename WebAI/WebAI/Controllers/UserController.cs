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


        public ActionResult UsersList(int roleId)
        {
            ViewBag.RoleId = roleId;
            return View(GetUsers());
        }

        IEnumerable<UserViewModel> GetUsers()
        {
            return _mapper.Map<IEnumerable<UserDTO>, IEnumerable<UserViewModel>>(_userService.GetUsers());
        }

        EditUserViewModel GetUser(int id)
        {
            return _mapper.Map<UserDTO, EditUserViewModel>(_userService.GetUserById(id));
        }

        public ActionResult DeleteUser(int id, int userRoleId)
        {
            _userService.DeleteUserById(id);
            return RedirectToAction("UsersList", new { roleId = userRoleId });
        }

        [HttpGet]
        public ActionResult EditUser(int id, int userRoleId)
        {
            ViewBag.RoleId = userRoleId;
            var user = GetUser(id);
            return View(user);
        }

        [HttpPost]
        public ActionResult EditUser(EditUserViewModel editUserModel, int userRoleId)
        {
            var user = _mapper.Map<EditUserViewModel, UserDTO>(editUserModel);
            _userService.EditUser(user);
            return RedirectToAction("UsersList", new { roleId = userRoleId });
        }

        public ActionResult UserAccountManage(int userId, int userRoleId)
        {
            ViewBag.RoleId = userRoleId;
            return View(GetUser(userId));
        }
    }
}