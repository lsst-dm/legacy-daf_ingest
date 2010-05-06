import lsst.daf.persistence as dafPersist
from lsst.obs.cfht import CfhtMapper

bf = dafPersist.ButlerFactory(mapper=CfhtMapper(root="."))
butler = bf.create()
psf = butler.get("psf", visit=788965, filter="r", ccd=6)
print psf.getKernel().toString()
