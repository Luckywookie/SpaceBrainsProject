from flask_restful import Resource, reqparse
from flask_jwt import jwt_required, current_identity
from models.pages import PageModel
from models.site import SiteModel
from models.rank import RankModel
from datetime import datetime

class Pages(Resource):
    @jwt_required()
    def get(self, id=None):
        json = 'None'
        if id:
            json = PageModel.find_by_id(id, current_identity.id)
        if json:
            return json
        return {'message': 'not found pages for this sites'}, 404

class StatList(Resource):
    @jwt_required()
    def get(self):
        result = SiteModel.query.filter_by(admin=current_identity.id).all()
        if result:
            return {'base statistic': [PageModel.find_by_id(el.id, current_identity.id) for el in result if
                                       PageModel.find_by_id(el.id, current_identity.id)]}
        return {'message': 'not found base statistic'}, 404


class Rank(Resource):
    @classmethod
    def _pars_date(self, date):
        # pattern = r'\d{2}-\d{2}-\d{4}'
        # if re.match(pattern, date) is not None:
        #   return True
        # return False
        try:
            # date_str = "30-10-2016 16:18"
            # format_str = "%d-%m-%Y %H:%M"
            format_str = "%Y-%m-%d"
            return datetime.strptime(date, format_str)
        except ValueError:
            return False

    @jwt_required()
    def get(self, id=None, date=None, date1=None, date2=None):
        json = 'None'
        if id:
            json = RankModel.find_by_id(id, current_identity.id)
        if id and date:
            date = self._pars_date(date)
            if not date:
                return {'message': 'format of date is false'}, 404
            json = RankModel.find_by_id(id, current_identity.id, date)
        if id and date1 and date2:
            date1 = self._pars_date(date1)
            date2 = self._pars_date(date2)
            if not date1 or not date2:
                return {'message': 'format of date is false'}, 404
            json = RankModel.find_by_id(id, current_identity.id, None, date1, date2)
        if json:
            return json
        return {'message': 'not found rank for this site'}, 404

class RankList(Resource):
    @jwt_required()
    def get(self, date=None, date1=None, date2=None):
        if date:
            date = RankDay._pars_date(date)
            if not date:
                return {'message': 'format of date is false'}, 404
        if date1 and date2:
            date1 = RankDay._pars_date(date1)
            date2 = RankDay._pars_date(date2)
            if not date1 or not date2:
                return {'message': 'format of date is false'}, 404

        result = SiteModel.query.filter_by(admin=current_identity.id).all()
        if result:
            return {
                'rank statistic': [RankModel.find_by_id(el.id, current_identity.id, date, date1, date2) for el in result
                                   if RankModel.find_by_id(el.id, current_identity.id, date, date1, date2)]}
        return {'message': 'not found base_rank statistic'}, 404