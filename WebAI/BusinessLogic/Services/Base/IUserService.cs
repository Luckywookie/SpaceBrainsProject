using BusinessLogic.DTO.Account;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BusinessLogic.Services.Base
{
    public interface IUserService
    {
        IEnumerable<UserDTO> GetUsers();
        UserDTO GetUserById(int id);

        void DeleteUserById(int id);
        
        void EditUser(UserDTO userDTO);
    }
}
