"""
MIVOT vocabulary and regular expressions.
"""
import re
from astropy import units as u
from pyvo.utils import prototype_feature


class Constant:
    """
    Class used to set constant to identify XML attributes added to the MIVOT ATTRIBUTES
    """
    FIRST_TABLE = "first_table"
    FIELD_UNIT = "field_unit"
    COL_INDEX = "col_index"
    ROOT_COLLECTION = "root_collection"
    NOT_SET = "NotSet"
    ANONYMOUS_TABLE = "AnonymousTable"


# Regexp pattern to check no valid mapping is present
NoMapping = re.compile(r".REPORT\s+status=['\"]KO")

unit_mapping = {
    "deg": u.degree,
    "rad": u.radian,
    "hourangle": u.hourangle,
    "arcsec": u.arcsec,
    "mas": u.mas,
    "pc": u.pc,
    "km": u.km,
    "m": u.m,
    "mas/yr": u.mas / u.yr,
    "mas/y": u.mas / u.yr,
    "km/s": u.km / u.s,
}


regex_patterns = {'iso': r'^(-?(?:[1-9][0-9]*)?[0-9]{4})-(1[0-2]|0[1-9])-(3[01]|0[1-9]|[12]'
                         r'[0-9]) (2[0-3]|[01][0-9]):([0-5][0-9]):([0-5][0-9])(\.[0-9]+)?'
                         r'(Z|[+-](?:2[0-3]|[01][0-9]):[0-5][0-9])?$',
                  'isot': r'^(-?(?:[1-9][0-9]*)?[0-9]{4})-(1[0-2]|0[1-9])-(3[01]|0[1-9]|[12][0-9])'
                          r'T(2[0-3]|[01][0-9]):([0-5][0-9]):([0-5][0-9])(\.[0-9]+)?(Z|[+-]'
                          r'(?:2[0-3]|[01][0-9]):[0-5][0-9])?$',
                  'yday': r'^(\d{4}):(\d{3}):(\d{2}):(\d{2}):(\d{2}\.\d+)$',
                  'ymdhms': r"^\{'year': (\d+), 'month': (\d+), 'day': (\d+)"
                            r"(?:, 'hour': (\d+))?(?:, 'minute': (\d+))?(?:, 'second': (\d+))?\}$"}


regex_format = {'iso': re.compile(regex_patterns['iso']),
                'isot': re.compile(regex_patterns['isot']),
                'yday': re.compile(regex_patterns['yday']),
                'ymdhms': re.compile(regex_patterns['ymdhms'])}


@prototype_feature('MIVOT')
class Ele:
    """
    Constant used to identify MIVOT Element
    """
    namespace = ""
    VODML = namespace + "VODML"
    MODEL = namespace + "MODEL"
    GLOBALS = namespace + "GLOBALS"
    TEMPLATES = namespace + "TEMPLATES"
    INSTANCE = namespace + "INSTANCE"
    ATTRIBUTE = namespace + "ATTRIBUTE"
    COLLECTION = namespace + "COLLECTION"
    JOIN = namespace + "JOIN"
    REFERENCE = namespace + "REFERENCE"
    WHERE = namespace + "WHERE"
    NOROLE = "NOROLE"


@prototype_feature('MIVOT')
class Att:
    """
    Constant used to identify attributes in MIVOT Element
    """
    dmrole = "dmrole"
    dmtype = "dmtype"
    dmid = "dmid"
    name = "name"
    value = "value"
    dmref = "dmref"
    tableref = "tableref"
    sourceref = "sourceref"
    ref = "ref"
    primarykey = "PRIMARY_KEY"
    foreignkey = "foreignkey"


@prototype_feature('MIVOT')
class MangoRoles:
    """
    Place holder for the MANGO draft roles
    """
    LONGITUDE = "longitude"
    LATITUDE = "latitude"
    PM_LONGITUDE = "pmLongitude"
    PM_LATITUDE = "pmLatitude"
    PARALLAX = "parallax"
    RADIAL_VELOCITY = "radialVelocity"
    EPOCH = "epoch"
    FRAME = "frame"
    EQUINOX = "equinox"
    PMCOSDELTAPPLIED = "pmCosDeltApplied"


EpochPropagation_fields = ["longitude", "latitude", "pmLongitude", "pmLatitude",
                           "radialVelocity", "parallax", "epoch", "frame", "equinox"]


skycoord_param_default = {
    MangoRoles.LONGITUDE: 'ra', MangoRoles.LATITUDE: 'dec', MangoRoles.PARALLAX: 'distance',
    MangoRoles.PM_LONGITUDE: 'pm_ra_cosdec', MangoRoles.PM_LATITUDE: 'pm_dec',
    MangoRoles.RADIAL_VELOCITY: 'radial_velocity', MangoRoles.EPOCH: 'obstime',
    MangoRoles.FRAME: 'frame'}

skycoord_param_fk4_fk5 = {
    MangoRoles.LONGITUDE: 'ra', MangoRoles.LATITUDE: 'dec', MangoRoles.PARALLAX: 'distance',
    MangoRoles.PM_LONGITUDE: 'pm_ra_cosdec', MangoRoles.PM_LATITUDE: 'pm_dec',
    MangoRoles.RADIAL_VELOCITY: 'radial_velocity', MangoRoles.EPOCH: 'obstime',
    MangoRoles.FRAME: 'frame', MangoRoles.EQUINOX: 'equinox'}

skycoord_param_galactic = {
    MangoRoles.LONGITUDE: 'l', MangoRoles.LATITUDE: 'b', MangoRoles.PARALLAX: 'distance',
    MangoRoles.PM_LONGITUDE: 'pm_l_cosb', MangoRoles.PM_LATITUDE: 'pm_b',
    MangoRoles.RADIAL_VELOCITY: 'radial_velocity', MangoRoles.EPOCH: 'obstime',
    MangoRoles.FRAME: 'frame'}


@prototype_feature('MIVOT')
def key_match(searched_key, key_set):
    """
    Check if any key in the key_set starts with the searched_key.
    Parameters
    ----------
    searched_key : str
        The key to search for.
    key_set : str, list, or odict_keys
        The set of keys to check.
    Returns
    -------
    str or None
        The matched key if found, otherwise None.
    """
    if isinstance(key_set, str):
        return key_set.startswith(searched_key)
    else:
        # May be a list or an odict_keys
        for key in key_set:
            if key.startswith(searched_key) is True:
                return key
    return None
