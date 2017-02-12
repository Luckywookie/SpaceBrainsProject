using BusinessLogic.Services.Base;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BusinessLogic.DTO.Account;
using AutoMapper;
using DataAccess.Repositories.Base;
using Domain.Entities.Users;

namespace BusinessLogic.Services
{
    public class UserService
        : IUserService
    {
        IUserRepository _userRepository = null;
        IMapper _mapper = null;

        public UserService(IUserRepository userRepository, IMapper mapper)
        {
            _userRepository = userRepository;
            _mapper = mapper;
        }

        public void DeleteUserById(int id)
        {
            _userRepository.DeleteUserById(id);
        }

        public void EditUser(UserDTO userDTO)
        {
            _userRepository.EditUser(_mapper.Map<UserDTO, User>(userDTO));
        }

        public UserDTO GetUserById(int id)
        {
            return _mapper.Map<User, UserDTO>(_userRepository.GetUserById(id));
        }

        public IEnumerable<UserDTO> GetUsers()
        {
            return _mapper.Map<IEnumerable<User>, IEnumerable<UserDTO>>(_userRepository.GetUsers());
        }
    }
}
