from db import db
from sqlalchemy.orm import synonym
from models.site import SiteModel
from models.pages import PageModel
from models.person import PersonModel
from sqlalchemy.sql.expression import func
from datetime import datetime

class RankModel(db.Model):
    __tablename__ = 'PersonPageRank'

    PersonID = db.Column(
        db.Integer,
        db.ForeignKey('Persons.ID'),
        primary_key=True
    )
    PageID = db.Column(
        db.Integer,
        db.ForeignKey('Pages.ID'),
        primary_key=True
    )
    Person = db.relationship('PersonModel')
    Page = db.relationship('PageModel')
    Rank = db.Column(db.Integer)

    page = synonym('PageID')
    person = synonym('PersonID')
    rank = synonym('Rank')

    def __init__(self, person, page, rank):
        self.page = page
        self.person = person
        self.rank = rank

    def json(self):
        return {'person_id': self.person, 'rank': self.rank}

    @classmethod
    def _query(self, id, site_id, date, date1, date2):
        query = db.session.query(func.sum(RankModel.rank))
        query = query.join(PageModel, RankModel.page == PageModel.id)
        query = query.join(SiteModel, PageModel.site_id == SiteModel.id)
        query = query.join(PersonModel, RankModel.person == PersonModel.id)
        if date1 and date2:
            return query.filter(RankModel.person == id,
                                SiteModel.id == site_id,
                                func.DATE(PageModel.scan) >= date1,
                                func.DATE(PageModel.scan) <= date2)
        if date:
            return query.filter(RankModel.person == id,
                                SiteModel.id == site_id,
                                func.DATE(PageModel.scan) == date)
        return query.filter(RankModel.person == id, SiteModel.id == site_id)  # mb PageModel.site_id == site_id

    @classmethod
    def _get_rank_for_person(self, id, site_id, date, date1, date2):
        query = self._query(id, site_id, date, date1, date2)
        query = query.one()
        if query[0] or str(query[0]) == '0':
            return str(query[0])
        return 'not found statistic for this person'

    @classmethod
    def find_by_id(cls, id, permission, date=None, date1=None, date2=None):
        result = SiteModel.query.filter_by(id=id, admin=permission).first()
        if result:
            return {'id': result.id,
                    'name': result.name,
                    'persons': [{'id': el.id,
                                 'name': el.name,
                                 'rank': cls._get_rank_for_person(el.id, result.id, date, date1, date2)} for el in
                                PersonModel.query.filter_by(admin=permission).all()]}

    @classmethod
    def find_by_person(cls, person):
        return cls.query.filter_by(person=person).first()