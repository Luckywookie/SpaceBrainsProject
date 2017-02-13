using AutoMapper;
using BusinessLogic.DTO.Account;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using WebAI.Models.Account;

namespace WebAI.Infrastructure.Mapping.Account
{
    public class UserViewProfile
        : Profile
    {
        public UserViewProfile()
        {
            CreateMap<UserRegistrationViewModel, UserDTO>()
                .ForMember(dest => dest.RoleId, opt => opt.Ignore())
                .ForMember(dest => dest.AdminId, opt => opt.Ignore());

            CreateMap<UserDTO, UserViewModel>();

            CreateMap<UserDTO, EditUserViewModel>()
                .ForMember(dest => dest.PasswordConfirm, opt => opt.MapFrom(src => src.Password));

            CreateMap<EditUserViewModel, UserDTO>();


        }
    }
}