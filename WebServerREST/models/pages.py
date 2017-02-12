from db import db
from sqlalchemy.orm import synonym
from models.site import SiteModel

class PageModel(db.Model):
    __tablename__ = 'Pages'

    ID = db.Column(db.Integer, primary_key=True, autoincrement=True)
    Url = db.Column(db.String(2048))
    FoundDateTime = db.Column(db.DateTime)
    LastScanDate = db.Column(db.DateTime)
    SiteID = db.Column(db.Integer, db.ForeignKey('Sites.ID'))

    # Site = db.relationship('SiteModel')
    id = synonym('ID')
    url = synonym('Url')
    found = synonym('FoundDateTime')
    scan = synonym('LastScanDate')
    site_id = synonym('SiteID')
    persons = db.relationship('RankModel', lazy='dynamic')

    def __init__(self, url, found, scan, site_id):
        self.url = url
        self.found = found
        self.scan = scan
        self.site_id = site_id

    @classmethod
    def find_by_id(cls, id, permission):
        result = SiteModel.query.filter_by(id=id, admin=permission).first()
        if result:
            total_count = cls.query.filter(PageModel.site_id == result.id).count()
            total_count_not_round = cls.query.filter(PageModel.site_id == result.id, PageModel.scan == None).count()
            total_count_round = cls.query.filter(PageModel.site_id == result.id, PageModel.scan != None).count()
            return {
                'id': result.id,
                'site': result.name,
                'total_count': total_count if total_count else 0,
                'total_count_not_round': total_count_not_round if total_count_not_round else 0,
                'total_count_round': total_count_round if total_count_round else 0
            }