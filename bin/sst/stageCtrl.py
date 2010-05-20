"""Code and constants to control which stages the ImgChar_*.py functions run"""
#
# Stages to run
#
DETECT =                0x1
MEASURE = DETECT        << 1
PSF = MEASURE           << 1
WCS = PSF               << 1
WCS_VERIFY = WCS        << 1
PHOTO_CAL = WCS_VERIFY  << 1
ALL_STAGES = (PHOTO_CAL << 1) - 1

def setPrerequisites(stages):
    """Given a desired set of stages, add in any pre-requisites that must also be run"""
    
    if not stages:
        return ALL_STAGES

    if stages & (WCS_VERIFY | PHOTO_CAL):
        stages |= WCS
    if stages & WCS:
        stages |= MEASURE
    if stages & MEASURE:
        stages |= DETECT

    return stages
