""" pull down blogger site for Let's Brew List and generate table of recipes """

import datetime
import time
import feedparser
import logging
from StringIO import StringIO
import threading
import webapp2

from google.appengine.ext import db
from google.appengine.ext import deferred
from google.appengine.api import urlfetch
from google.appengine.api import users
from google.appengine.runtime import DeadlineExceededError

import jinja2, os

FEED_CHUNK_SIZE = 25

feedparser.RESOLVE_RELATIVE_URIS = 0
feedparser.SANITIZE_HTML = 0
feedparser.PARSE_MICROFORMATS = 0

# black list of posts to ignore
BLACK_LIST = ('2011-11-01',)

# set template
jinja_environment = jinja2.Environment(loader=jinja2.FileSystemLoader(os.path.dirname(__file__)))

class LastUpdated(db.Model):
    """ config data for our app """
    # when I last did an actual RSS update pull and update
    when = db.DateTimeProperty()
    # updated from RSS feed, used to detect when to actually pull new RSS feed
    last_updated = db.StringProperty()
    # cached totalResults to let us know what max to use when re-fetching all requests
    max_results = db.IntegerProperty()
    # updated field in actual XML - another check for updates
    xml_updated = db.DateTimeProperty()

    # HTTP Headers
    etag = db.StringProperty()
    last_modified = db.StringProperty()

    _INSTANCE = None

    @classmethod
    def get_instance(cls):
        if not cls._INSTANCE:
            cls._INSTANCE = cls.get_or_insert("only_one")
        return cls._INSTANCE

class LetsBrew(db.Model):
    """ models a single Let's Brew Entry """
    title = db.StringProperty()
    link = db.LinkProperty()
    comment = db.StringProperty()
    comment_link = db.LinkProperty()
    date = db.DateProperty()

class BrewPost(object):
    """ intermediate class between RSS data and GAE Datastore """

    def __init__(self, entry, comment_link):
        self.title = entry.title
        self.published_parsed = entry.published_parsed
        self.link = entry.link
        if comment_link:
            self.comment_title = comment_link['title']
            self.comment_link = self.link + "#comments"
        else:
            self.comment_title = None
            self.comment_link = None
        pp = self.published_parsed
        self.date = datetime.date(pp[0], pp[1], pp[2])
        self.datestr = self.date.isoformat()
        self.id_datestr = self.date.strftime("%Y%m%d")

    def update_db(self):
        """ update me into the datastore """
        key_name = self.id_datestr

        lb = LetsBrew.get_or_insert(key_name)

        if lb.is_saved():
            #logging.info("found existing %s (%s)" %(lb.title, lb.key().name()))
            pass

        lb.title = self.title
        lb.link = self.link
        lb.comment = self.comment_title
        lb.comment_link = self.comment_link
        lb.date = self.date

        lb.put()

    def drop_db(self):
        """ update me into the datastore """
        key_name = self.id_datestr

        lb = LetsBrew.get_by_key_name(key_name)

        if lb:
            if lb.is_saved():
                lb.delete()

def parse_entry_block(rss):
    """ find all of our interesting posts """
    entries = []
    for en in rss.entries:
        iwant = False
        for ta in en.get('tags', ()):
            if ta.term.find("Let's Brew") != -1 or ta.term.find("Beer Recipes") != -1:
                iwant = True

        if en.title.find("Let's Brew") != -1:
            iwant = True

        if iwant:
            comment = None
            # find comments link to add as well
            for link in en.links:
                if link.has_key("title") and link["type"].find("text/html") != -1:
                    title = link["title"]
                    idx = link["title"].find("Comments")
                    if idx != -1:
                        comment = link
                        break
            bp = BrewPost(en, comment)
            ts = ("Let's Brew Wednesday - ", "Let's brew Wednesday - ", "Let's Brew ", "Let's brew ")
            title = ""
            for t in ts:
                if bp.title.startswith(t):
                    bp.title = bp.title[len(t):]
                    break
            if not bp.title:
                logging.info("skipping %s, doesn't appear to be valid (%s)" %(bp.title, bp.link))
                bp.drop_db()
                continue
            if bp.datestr in BLACK_LIST:
                logging.info("skipping %s, in black list" %(bp.title,))
                bp.drop_db()
                continue
            entries.append(bp)
            bp.update_db()
    return entries

def rpc_callback(rpc, start):
    """ process rpc on callback for completion """
    #logging.debug("processing rpc for start %d, %s" %(start, rpc))
    try:
        feed = rpc.get_result()
        if feed.status_code == 200:
            headers = feed.headers
            etag = headers.get('ETag', None)
            last_modified = headers.get('Last-Modified', None)
            rss = feedparser.parse(StringIO(feed.content))
            if start == 1:
                xml_updated = rss.feed.updated_parsed
                dt = datetime.datetime(*xml_updated[:6])
                last = LastUpdated.get_instance()
                if last_modified is not None:
                    last.etag = etag
                    last.last_modified = last_modified
                last.xml_updated = dt
                last.put()
                logging.info("Stashed xml_updated %s etag %s last-modified %s" %(dt, etag, last_modified))
            entries = parse_entry_block(rss)
            #logging.debug("parsed %d, found %d", len(rss.entries), len(entries))
        else:
            logging.error("fetch returned %d" %(feed.status_code,))

    except urlfetch.Error, e:
        logging.error("error  %s" %(e,))

def create_callback(rpc, start):
    """ create anonymous callback to stash data """
    return lambda: rpc_callback(rpc, start)

def check_feed_changed():
    """ do simple pull to see if RSS feed changed since last pull """

    url = "http://barclayperkins.blogspot.com/feeds/posts/default"
    url += "?start-index=1&max-results=0"

    # send last headers to allow server to send 304 status_code for short-circuit
    last = LastUpdated.get_instance()
    if last.last_modified:
        hdrs = {'If-None-Match': last.etag, 'If-Modified-Since': last.last_modified}
        logging.info("Sending etag %s last-modified %s" %(last.etag, last.last_modified))
    else:
        hdrs = {}

    logging.info("fetching url %s" %(url,))
    try:
        feed = urlfetch.fetch(url, deadline=60, method=urlfetch.GET, headers=hdrs)
        if feed.status_code == 200:
            # check if etag drifting on us
            rss = feedparser.parse(StringIO(feed.content))
            xml_updated = rss.feed.updated_parsed
            dt = datetime.datetime(*xml_updated[:6])
            headers = feed.headers
            etag = headers.get('ETag', None)
            last_modified = headers.get('Last-Modified', None)
            logging.info("xml old %s new %s, etag %s last_modified %s" %(last.xml_updated, dt, etag, last_modified))
            if not last.xml_updated or last.xml_updated < dt:
                logging.info("status 200, xml_updated is old: new entries to get")
                return True
            else:
                logging.info("status 200, xml_updated is current: no new entries to get")
                return False
        elif feed.status_code == 304:
            logging.info("status 304, no new entries")
            return False
        else:
            logging.error("fetch returned %d" %(feed.status_code,))
            return True
    except urlfetch.Error, e:
        logging.error("error  %s" %(e,))
    return True

def request_entries(start, num_entries):
    """ setup RPC request to pull request @ start """
    #logging.debug("requesting entries %d to %d" %(start, start + num_entries))

    url = "http://barclayperkins.blogspot.com/feeds/posts/default"
    url += "?start-index=%d&max-results=%d" %(start, num_entries)

    # send last headers to allow server to send 304 status_code for short-circuit
    hdrs = {}
    if start == 1:
        last = LastUpdated.get_instance()
        if last.etag:
            hdrs = {'If-None-Match': last.etag, 'If-Modified-Since': last.last_modified}
            logging.info("Sending etag %s last-modified %s" %(last.etag, last.last_modified))

    rpc = urlfetch.create_rpc(deadline=300)
    rpc.callback = create_callback(rpc, start)

    logging.info("fetching url %s" %(url,))
    urlfetch.make_fetch_call(rpc, url, method=urlfetch.GET, headers=hdrs)
    return rpc

class EntryUpdater(object):
    """ deferred entry updating """

    def run(self, start=1, end=2000):
        """ fetch the feed, parse the entries and store in DB """
        start_idx = range(start, end, FEED_CHUNK_SIZE)
        logging.info("start: %d, list %s" %(start, start_idx))

        try:
            for start in start_idx:
                rpc = request_entries(start, FEED_CHUNK_SIZE)

                #logging.info("all rpcs requested, waiting...")
                try:
                    rpc.check_success()
                except urlfetch.Error, e:
                    logging.error("rpc %s took error %e" %(rpc, e))

        except DeadlineExceededError:
            logging.info("hit DeadlineExceeded at start=%s, rescheduling" %(start,))
            deferred.defer(self.run, start, end)

        last = LastUpdated.get_instance()
        last.when = datetime.datetime.now()
        last.put()

        logging.debug("all rpcs complete")

def update_entries(start, end):
    eu = EntryUpdater()
    deferred.defer(eu.run, start, end)

def get_entries():
    """ see what we have in the database... """
    query = LetsBrew.all()
    query.order('-date')
    return query

class UpdatePage(webapp2.RequestHandler):
    update_start = 1
    update_end = 150

    def update_func(self):
        if self.update_start == 1 and not check_feed_changed():
            # nothing new at the start
            return
        update_entries(self.update_start,self.update_end)

    def get(self):
        user = users.get_current_user()
        self.response.headers['Content-Type'] = 'text/html'
        entries = self.update_func()
        # even though we have admin required for the update urls, cron doesn't supply a real user
        if user:
            greeting = ("Welcome, %s! (<a href=\"%s\">sign out</a>)" % (user.nickname(),
                                                                        users.create_logout_url("/")))
        else:
            greeting = ("Hello, Mr. Cron!")

        self.response.out.write("<html><body>%s</body></html>" % greeting)

class UpdateWeeklyPage(UpdatePage):
    update_start = 150
    update_end = 500

class UpdateAllPage(UpdatePage):
    update_start = 500
    update_end = 2000

class MainPage(webapp2.RequestHandler):
    def get(self):
        entries = get_entries()
        last = LastUpdated.get_instance()
        template_values = {
            'entries': entries,
            'last': last,
        }

        template = jinja_environment.get_template('brewlist.html')
        self.response.out.write(template.render(template_values))

app = webapp2.WSGIApplication([('/', MainPage),
                               ('/update/', UpdatePage),
                               ('/update/weekly', UpdateWeeklyPage),
                               ('/update/all', UpdateAllPage)], debug=True)

# TODO
