from mangum import Mangum

from saaslite.api import app

handler = Mangum(app, lifespan='off')
