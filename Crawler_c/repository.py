import pymysql

# import sqlite3
import datetime

conn = pymysql.connect(
    host='localhost',
    user='root',  # Мои настройки для БД
    password='root',  # Мои настройки для БД
    db='ratepersons',
    charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor)

cursor = conn.cursor()


class Entity:
    """Класс для оздания метода представления сущностей"""

    def __init__(self):
        pass

    @property
    def value(self):
        return self.__dict__


class Person(Entity):
    """Класс описыввает сущность Person"""

    def __init__(self, ID=0, Name=None):
        super(Entity, self).__init__()
        self.ID = ID
        self.name = Name


class Keyword(Entity):
    """Класс описыввает сущность Keyword"""

    def __init__(self, ID=0, Name=None, PersonID=None):
        super(Entity, self).__init__()
        self.ID = ID
        self.Name = Name
        self.PersonID = PersonID


class Site(Entity):
    """Класс описыввает сущность Site"""

    def __init__(self, ID=0, Name=None):
        super(Entity, self).__init__()
        self.ID = ID
        self.Name = Name


class Page(Entity):
    """Класс описыввает сущность Page"""

    def __init__(self, ID=0, Url=None, SiteID=0, FoundDateTime=None, LastScanDate=None):
        super(Entity, self).__init__()
        self.ID = ID
        self.Url = Url
        self.SiteID = SiteID
        self.FoundDateTime = FoundDateTime
        self.LastScanDate = LastScanDate


class PersonPageRank(Entity):
    """Класс описыввает сущность PersonPageRank"""

    def __init__(self, PersonID=0, PageID=0, Rank=0):
        super(Entity, self).__init__()
        self.PersonID = PersonID
        self.PageID = PageID
        self.Rank = Rank


class FakeKeywordRepository:
    """Класс фекогового репозитория Krywords"""

    def __init__(self):
        pass

    def getkeywordbypersonid(self, personid):
        keywords = [
            Keyword(1, 'Путина', 1),
            Keyword(2, 'Путине', 1),
            Keyword(3, 'Путину', 1),
            Keyword(4, 'Медведев', 2)
        ]
        return [item.value for item in keywords if item.value['PersonID'] == personid]


class DbKeywordRepository:
    """Класс репозитория Krywords работающий с БД"""

    def __init__(self):
        pass

    def getkeywordbypersonid(self, personid):
        sql = "select * from `Keywords` where `Keywords`.`PersonID` = %s"
        cursor.execute(sql, (personid,))
        result = cursor.fetchall()
        return result
        # keywords = []
        # return [item.value for item in keywords if item.value['PersonID'] == personid]


class DbPersonRepository:
    """Класс репозитория Person работающий с БД"""
    def __init__(self):
        pass


    def getpersons(self):
        sql = "select * from `Persons`"
        cursor.execute(sql)
        result = cursor.fetchall()
        return result


class SiteRepositoryWorker:
    """Класс реализует взаимодействие с репозиторием Site"""

    def __init__(self, repository):
        self.repository = repository


    def getapersons(self):
        result = [item for item in self.repository.getpersites()]
        return result

    def getsitestorank(self):
        result = [item for item in self.repository.getsitestorank()]
        return result


class DbSiteReposytory:
    """Класс репозитория Site работающий с БД"""

    def __init__(self):
        pass

    def getsites(self):
        sql = "select * from `Sites`"
        cursor.execute(sql)
        result = cursor.fetchall()
        return result

    def getsitestorank(self):
        sql = 'select * from sites where id not in (select distinct siteid from pages)'
        cursor.execute(sql)
        result = cursor.fetchall()
        return result


class PersonRepositoryWorker:
    """Класс реализует взаимодействие с репозиторием Person"""

    def __init__(self, repository):
        self.repository = repository

    def getpersons(self):
        result = [item for item in self.repository.getpersons()]
        return result


class KeywordRepositoryWorker:
    """Класс реализует взаимодействие с репозиторием Keyword"""

    def __init__(self, repository):
        self.repository = repository

    def getbypersonid(self, personid):
        return [item for item in self.repository.getkeywordbypersonid(personid)]


class PagesRepositoryWorker:
    """Класс реализует взаимодействие с репозиторием Pages"""

    def __init__(self, repository):
        self.repository = repository

    def getallpages(self):
        result = [item for item in self.repository.getallpages()]
        return result

    def getsiteidfrompages(self):
        result = [item for item in self.repository.getsiteidfrompages()]
        return result

    def getpagelastscandatenull(self):
        result = [item for item in self.repository.getpagelastscandatenull()]
        return result

    def writepagestostore(self, page):
        self.repository.writepagestostore(page)

    def updatepageinstore(self, page):
        self.repository.updatepageinstore(page)


class DbPageRepository:
    """Класс репозитория Page работающий с БД"""

    def __init__(self):
        pass

    def getallpages(self):
        sql = "select * from `Pages`"
        cursor.execute(sql)
        result = cursor.fetchall()
        return result

    def getsiteidfrompages(self):
        sql = "select distinct `siteid` from `Pages`"
        cursor.execute(sql)
        result = cursor.fetchall()
        return result

    def getpagelastscandatenull(self):
        sql = "select * from `Pages` where `Pages`.`LastScanDate` is null"
        cursor.execute(sql)
        result = cursor.fetchall()
        return result

    def writepagestostore(self, page):
        sql = "insert into `Pages` (Url, SiteID, FoundDateTime) values (%s, %s, %s)"
        param = (page.value['Url'], page.value['SiteID'], page.value['FoundDateTime'])
        cursor.execute(sql, (param))
        conn.commit()

    def updatepageinstore(self, page):
        sql = 'update `Pages` set `LastScanDate`=%s where `Pages`.`ID` = %s'
        param = (page.value['LastScanDate'], page.value['ID'])
        cursor.execute(sql, (param))
        conn.commit()


class DbPersonPageRankRepository:
    """Класс репозитория PersonPageRank работающий с БД"""

    def __init__(self):
        pass

    def writeranktostore(self, personpagerank):
        sql = 'insert into `personpagerank` (personid, pageid, rank) values (%s, %s, %s)'
        param = (personpagerank.value['PersonID'], personpagerank.value['PageID'], personpagerank.value['Rank'])
        cursor.execute(sql, (param))
        conn.commit()

    def updaterankinstore(self, page):
        pass
        '''
        sql = 'update `Pages` set `LastScanDate`=%s where `Pages`.`ID` = %s'
        param = (page.value['LastScanDate'], page.value['ID'])
        cursor.execute(sql, (param))
        conn.commit()
        '''


class PersonPageRankRepositoryWorker:
    """Класс реализует взаимодействие с репозиторием PersonPageRankRepository"""

    def __init__(self, repository):
        self.repository = repository

    def writeranktostore(self, page):
        self.repository.writeranktostore(page)


def main():
    keywordrepo = FakeKeywordRepository()
    k = KeywordRepositoryWorker(keywordrepo)
    k1 = k.getbypersonid(1)
    print(k1)

    # k1 = Keyword(1, 'Вася', 1)
    # print ( k1.value )

    pagesrepo = DbPageRepository()
    p = PagesRepositoryWorker(pagesrepo)
    p1 = p.getpagelastscandatenull()

    print(p1)

    sitesrepo = DbSiteReposytory()
    s = SiteRepositoryWorker(sitesrepo)
    s1 = s.getsitestorank()

    print(s1)
    # pp = Page( Url = 'http://lenta.ru/sitemap.xml', SiteID = 2, FoundDateTime = datetime.datetime.today() )
    # print( pp.value )
    # p.writepagestostore(pp)
    # pp = Page(_id=3687, lastscandate=datetime.datetime.today() )
    # p.updatepageinstore(pp)


if __name__ == '__main__':
    main()
