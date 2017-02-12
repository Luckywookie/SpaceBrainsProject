using DataAccess.Repositories.Base;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DataAccess.Context;
using Domain.Entities.Users;

namespace DataAccess.Repositories
{
    public class UserRepository
        : BaseRepository, IUserRepository

    {
        public UserRepository(WebAIDbContext context)
            : base(context)
        {
        }

        public void DeleteUserById(int id)
        {
            User user = GetUserById(id);
            if (user == null)
                return;
            _context.Users.Remove(user);
            _context.SaveChanges();
        }

        public void EditUser(User user)
        {
            if (user == null)
                return;
            //user.AdminId = GetCurrentUserId();
            _context.Entry(user).State = System.Data.Entity.EntityState.Modified;
            _context.SaveChanges();
        }

        public User GetUserById(int id)
        {
            return _context.Users.Find(id);
        }

        public IEnumerable<User> GetUsers()
        {
            return GetUsersByAdminId();
        }

        IEnumerable<User> GetUsersByAdminId()
        {
            string name = GetCurrentUserName();
            return _context.Users.Where(x => x.Admin.Login == name).ToArray();
        }
    }
}
