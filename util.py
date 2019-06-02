#!/usr/env/python

import binascii
import random
import json
import pprint
import datetime
import sys
from collections import defaultdict
from collections import Counter

# Specify to which point the analysis goes / should go
#CURRENT_BLOCKHEIGHT = 514240
CURRENT_BLOCKHEIGHT = 556400
#current_blockheight = 471892
DIFFICULTY_PERIOD = 2016
BLOCK_DAY = 144

# Keys of blocks_*.json dict
D_TIME="time"
D_CB="cb"
D_ADDRESSES="addresses" # list
D_MINER="miner"
D_CONFLICTS="conflicts"
D_ATTRIBUTION="attribution"
D_ATTRIBUTIONS="attributions"
# UPDATE 2019-01-28
D_HASH="hash"
D_PHASH="phash"
D_PAYOUT="payout"
# Keys of D_ATTRIBUTIONS
DD_BCI_ADDR_ATTR = "blockchain_info_address"
DD_BCI_MARK_ATTR = "blockchain_info_marker"
DD_BCI_ATTR = "blockchain_info"

DD_BTCCOM_ADDR_ATTR = "btccom_address"
DD_BTCCOM_MARK_ATTR = "btccom_marker"
DD_BTCCOM_ATTR = "btccom"

DD_BT_ATTR = "blocktrail_com"

DD_CB_MARKER = "coinbase_marker"

DD_CUSTOM_ADDR_ATTR = "custom_addr"
DD_CUSTOM_MARKER_ATTR = "custom_marker"
DD_CUSTOM_ATTR = "custom"

DD_GS_CLUSTER = "graphsense_cluster"
DD_GS_TAG = "graphsense_tag"
# Keys of each attribution
DDD_MINER="miner"
DDD_SRC="src"
DDD_CLUSTER="cluster"

# Keys of miners_*.json dict
D_MARKERS="markers"
D_NAMES="names"
D_ADDRESSES="addresses"
D_CONFIG="config"
# Keys of D_CONFIG
DD_COLOR="color"
# Keys of D_MARKERS,D_NAMES and D_ADDRESSES
DD_FIRSTUSED="firstUsed"
DD_LASTUSED="lastUsed"
DD_SOURCES="sources" # list
DD_CURRENCIES="currencies" # list
# Keys of D_NAMES
DD_FULLNAME = "fullName"
DD_URL = "url"

# Special key of matches
DD_MATCH = "match"
ADDR_MATCH = "addr_match"
CB_MATCH = "cb_match"

# Helper functions

def add_block(blocks,height,time=None,address=None,cb=None,bhash=None,phash=None,payout=None):
    """ Add block to blocks dict which gets persited to blocks_*.json file

    This is used to roduce the raw blocks file without attributions
    """
    if height not in blocks:
        blocks[ height ] = { D_TIME:time,
                             D_CB:cb,
                             D_ADDRESSES:[ address, ],
                             D_MINER:"",
                             D_CONFLICTS:0,
                             D_ATTRIBUTION:"",
                             D_ATTRIBUTIONS:dict(),
                             D_HASH: bhash,
                             D_PHASH: phash,
                             D_PAYOUT: payout }
        return True
    elif address not in blocks[ height ][ D_ADDRESSES ]:
        blocks[ height ][ D_ADDRESSES ].append( address )
        return True

    else:
       return False

def get_miner_payouts(blocks,start_height,end_height,miner_id):
    """ Sum up all payouts paied to the respective miner_id during interval

    Returns a tuple consisting of payout sum [0] and the number of blocks that matched [1]
    """
    assert start_height <= end_height

    sum_payouts = 0
    sum_blocks = 0

    for i in range(start_height,end_height+1):
        if str(i) in blocks:
            if ( D_MINER in blocks[ str(i) ].keys() and
                 D_PAYOUT in blocks[ str(i) ].keys() ):
                if blocks[ str(i) ][ D_MINER ] == miner_id:
                   sum_payouts += int(blocks[ str(i) ][ D_PAYOUT ])
                   sum_blocks += 1

    return (sum_payouts,sum_blocks)

def add_miner(miner_id,
              miners,
              names_dict=None,
              markers_dict=None,
              addresses_dict=None,
              update=False):
    """ Add or update miner in miner dict that gets persited to miners_*.json
    """

    # parameter set if something has successfully changed the miners dict
    changed = False

    if miner_id not in miners:
        # create miner_id if it does not exist yet
        miners[miner_id] = {D_NAMES:dict(), D_MARKERS:dict(), D_ADDRESSES:dict()}
        changed = True

    # assign dict entry for current pool
    current_miner = miners[miner_id]

    if names_dict:
        for name in names_dict.keys():
            if ( name not in current_miner[ D_NAMES ].keys() ) or update:
                current_miner[ D_NAMES ][name] = names_dict[name]
                changed = True
            elif names_dict[ name ][ DD_SOURCES ]:
                for source in names_dict[ name ][ DD_SOURCES ]:
                    if source not in current_miner[ D_NAMES ][ name ][ DD_SOURCES ]:
                        current_miner[ D_NAMES ][ name ][ DD_SOURCES ].append( source )
                        #current_miner[ D_NAMES ][ name ][ DD_FIRSTUSED ] = firstUsed
                        #current_miner[ D_NAMES ][ name ][ DD_LASTUSED ] = lastUsed
                        changed = True

    if markers_dict:
        for marker in markers_dict.keys():
            if ( marker not in current_miner[ D_MARKERS ].keys() ) or update:
                current_miner[ D_MARKERS ][marker] = markers_dict[marker]
                changed = True
            elif markers_dict[ marker ][ DD_SOURCES ]:
                for source in markers_dict[ marker ][ DD_SOURCES ]:
                    if source not in current_miner[ D_MARKERS ][ marker ][ DD_SOURCES ]:
                        current_miner[ D_MARKERS ][ marker ][ DD_SOURCES ].append( source )
                        #current_miner[ D_MARKERS ][ marker ][ DD_FIRSTUSED ] = firstUsed
                        #current_miner[ D_MARKERS ][ marker ][ DD_LASTUSED ] = lastUsed
                        changed = True

    if addresses_dict:
        for address in addresses_dict.keys():
            if ( address not in current_miner[ D_ADDRESSES ].keys() ) or update:
                current_miner[ D_ADDRESSES ][address] = addresses_dict[address]
                changed = True
            elif addresses_dict[ address ][ DD_SOURCES ]:
                for source in addresses_dict[ address ][ DD_SOURCES ]:
                    if source not in current_miner[ D_ADDRESSES ][ address ][ DD_SOURCES ]:
                        current_miner[ D_ADDRESSES ][ address ][ DD_SOURCES ].append( source )
                        #current_miner[ D_ADDRESSES ][ address ][ DD_FIRSTUSED ] = firstUsed
                        #current_miner[ D_ADDRESSES ][ address ][ DD_LASTUSED ] = lastUsed
                        changed = True
    return changed

def add_addr(miner,miners,addr,source,currencies=list(),firstUsed=0,lastUsed=0):
    """ Add another addr to a miner

    If already exits source is added if not already in the list, and update firstUsed lastUsed value
    """
    if miner not in miners.keys():
        return False

    if addr not in miners[ miner ][ D_ADDRESSES ].keys():
        miners[ miner ][ D_ADDRESSES ][ addr ] = { DD_SOURCES: [ source, ],
                                               DD_FIRSTUSED:firstUsed,
                                               DD_LASTUSED:lastUsed,
                                               DD_CURRENCIES:currencies }
    elif source not in miners[ miner ][ D_ADDRESSES ][ addr ][ DD_SOURCES ]:
        miners[ miner ][ D_ADDRESSES ][ addr ][ DD_SOURCES ].append( source )
        miners[ miner ][ D_ADDRESSES ][ addr ][ DD_FIRSTUSED ] = firstUsed
        miners[ miner ][ D_ADDRESSES ][ addr ][ DD_LASTUSED ] = lastUsed
    return miners[ miner ]


def add_name(miner,miners,name,source,currencies=list(),fullname="",url="",firstUsed=0,lastUsed=0):
    """ Add another name to a miner

    If already exits source is added if not already in the list, and update firstUsed and lastUsed value
    """
    if miner not in miners.keys():
        return False

    if name not in miners[ miner ][ D_NAMES ].keys():
        miners[ miner ][ D_NAMES ][ name ] = { DD_SOURCES: [ source, ],
                                               DD_FIRSTUSED:firstUsed,
                                               DD_LASTUSED:lastUsed,
                                               DD_FULLNAME:fullname,
                                               DD_CURRENCIES:currencies,
                                               DD_URL:url }
    elif source not in miners[ miner ][ D_NAMES ][ name ][ DD_SOURCES ]:
        miners[ miner ][ D_NAMES ][ name ][ DD_SOURCES ].append( source )
        miners[ miner ][ D_NAMES ][ name ][ DD_FIRSTUSED ] = firstUsed
        miners[ miner ][ D_NAMES ][ name ][ DD_LASTUSED ] = lastUsed
    return miners[ miner ]


def get_miner_id_by_name(miners,miner_name):
    """ Return the minder_id if miner_name has been found
    """
    miner_id = None
    miner_matches = list()

    for miner in miners:
        if miners[ miner ][ D_NAMES ]:
            for name in miners[ miner ][ D_NAMES ]:
                if name == miner_name:
                    miner_matches.append( miner )

    if len(miner_matches) > 1:
        raise InvalidMinerData("At leas two miner_ids have the same name",miner_matches[0],miner_matches[1],miners)
    elif len(miner_matches) == 1:
        miner_id = miner_matches[0]
    return miner_id


def add_marker(miner,miners,marker,source,currencies=list(),firstUsed=0,lastUsed=0):
    """ Add another marker to a miner

    If alread exists source is added if not already in the list
    """
    if miner not in miners.keys():
        return False

    if marker not in miners[ miner ][ D_MARKERS ].keys():
        miners[ miner ][ D_MARKERS ][ marker ] = { DD_SOURCES: [ source, ],
                                               DD_FIRSTUSED:firstUsed,
                                               DD_LASTUSED:lastUsed,
                                               DD_CURRENCIES:currencies }
    elif source not in miners[ miner ][ D_MARKERS ][ marker ][ DD_SOURCES ]:
        miners[ miner ][ D_MARKERS ][ marker ][ DD_SOURCES ].append( source )
        miners[ miner ][ D_MARKERS ][ marker ][ DD_FIRSTUSED ] = firstUsed
        miners[ miner ][ D_MARKERS ][ marker ][ DD_LASTUSED ] = lastUsed
    return miners[ miner ]


def get_sample(d,pp=False,ret=True):
    """ Get and optionally pretty print sample element of dict

    Parameters
    ----------
    d: dict
        The dict to sample from
    pp: bool
        If yes pretty print the random element

    Returns
    -------
    dict
        A sample from tfrom the dict
    """
    if not isinstance(d, dict):
        return None
    rkey = random.choice(list(d.keys()))
    if pp:
        print("key   = ",repr(rkey))
        print("value = ")
        pp = pprint.PrettyPrinter(indent=4)
        pp.pprint(d[ rkey ])
    if ret:
        return d[ rkey ]
    else:
        pass

class ConflictingMinerData(Exception):
    def __init__(self, message, miner1="", miner2="", address="", coinbase="",addr_match=None,cb_match=None):
        super().__init__(message)
        self.message = message
        self.miner1 = miner1
        self.miner2 = miner2
        self.address = address
        self.coinbase = coinbase
        self.addr_match = addr_match
        self.cb_match = cb_match

class CorruptedMinerData(Exception):
    pass

class InvalidMinerData(Exception):
    def __init__(self, message, miner1="", miner2="", miners=dict()):
        super().__init__(message)
        self.message = message
        self.miner1 = miner1
        self.miner2 = miner2
        self.miners = miners

def match_coinbase_to_miner(coinbase,miners,strict=True,blknum=None):
    """ Return all matching miners for markers in the respective coinbase

    Parameters
    ----------
    coinbase: string
        The repsective coinbase to search
    miners: dict
        The miners.json file as dict to look for markers
    strict: bool
        If False do not raise exception in case of multiple matches

    Returns
    -------
    list
        List of dict objects, one for each match
    """
    match = list()

    for miner in miners:
        # iterate over all markers in all miners
        for marker in miners[miner][ D_MARKERS ]:
            if isinstance(marker,str):
                mr = bytes( marker.encode("utf-8") ) # This should work in python2.7 and python3
            elif isinstance(marker,bytes):
                mr = marker
            else:
                raise CorruptedMinerData()
            cb = binascii.unhexlify( coinbase )
            if mr in cb:
                # check if cb has a validity period defined by DD_FIRSTUSED and DD_LASTUSED
                firstUsed = miners[miner][ D_MARKERS ][ marker ].get( DD_FIRSTUSED,0 )
                lastUsed = miners[miner][ D_MARKERS ][ marker ].get( DD_LASTUSED,0 )
                if ( ( blknum is None ) or
                     ( ( firstUsed <= blknum or firstUsed == 0 ) and ( lastUsed >= blknum or lastUsed == 0 ) ) ):
                    #match.append( ( miner, { marker: miners[miner]["markers"][marker], DD_MATCH:CB_MATCH } ) )
                    match.append( ( miner, { CB_MATCH:marker } ) )
                    #print("Match: " + miner + " marker: " + repr(mr) + " cb: " + repr(cb) )

    if len(match) > 1 and strict:
        # check if there are two marker/tag matches for different miners
        # should only be one miner
        matched_miner = match[0][0]
        for m in match:
            if m[0] != matched_miner:
                raise ConflictingMinerData("Multiple coinbase markers of different miners match",matched_miner,m[0],None,coinbase,None,match)

    return match

def match_address_to_miner(address,miners,strict=True,blknum=None):
    """ Return all matching miners for addresses

    Parameters
    ----------
    address: string
        The address to check
    miners: dict
        The miners.json file as dict to look for matching addresses
    strict: bool
        If False do not raise exception in case of multiple matches
    blknum: int
        If given, specifies the current block that should be matched.
        This is important to check with DD_FIRSTUSED / DD_LASTUSED

    Returns
    -------
    list
        List of dict objects, one for each match
    """
    match = list()

    for miner in miners:
        if address in miners[miner][ D_ADDRESSES ].keys():
            # check if address has a validity period defined by DD_FIRSTUSED and DD_LASTUSED
            firstUsed = miners[miner][ D_ADDRESSES ][ address ].get( DD_FIRSTUSED,0 )
            lastUsed = miners[miner][ D_ADDRESSES ][ address ].get( DD_LASTUSED,0 )
            if ( ( blknum is None ) or
                 ( ( firstUsed <= blknum or firstUsed == 0 ) and ( lastUsed >= blknum or lastUsed == 0 ) ) ):
                #match.append( ( miner, { address: miners[miner]["addresses"][address], DD_MATCH:ADDR_MATCH } ) )
                match.append( ( miner, { ADDR_MATCH:address } ) )

    if len(match) > 1 and strict:
        # if the address matches more than once there is an
        # error in our miners data
        raise ConflictingMinerData("Multiple addresses match",match[0][0],match[1][0],address,"",match,None)

    return match


def match_miner(miners,address=None,coinbase=None,update=False,blknum=None):
    """ Return a miner from miners json if address and/or coinbase matches

    Parameters
    ----------
    address: string

    miners: dict

    coinbase: string
        If coinbase is given markers are also checked

    update: bool
        If update is set and coinbase tag has been found but no address then miners dict is updated

    blknum: int
        If given, specifies the current block that should be matched.
        This is important to check with DD_FIRSTUSED / DD_LASTUSED

    Returns
    -------
    list
        List of dict objects, one for each match
    """
    match = list()

    if address:
        addr_match = match_address_to_miner(address,miners,strict=True,blknum=blknum)
    else:
        addr_match = list()

    if coinbase:
        # if a coinbase was given check the coinbase for markers
        cb_match = match_coinbase_to_miner(coinbase,miners,strict=True,blknum=blknum)
    else:
        cb_match = list()

    if len(addr_match) == 1 and len(cb_match) > 0:
        if addr_match[0][0] != cb_match[0][0]:
            raise ConflictingMinerData("Addr and Cb match differ",addr_match[0][0],cb_match[0][0],address,coinbase,addr_match,cb_match)

    if len(addr_match) == 0 and len(cb_match) > 0 and update and address:
        # update the matched miner with the address if marker matches but no address
        miner = cb_match[0][0]
        if address not in miners[miner][ D_ADDRESSES ]:
            miners[ miner ][ D_ADDRESSES ][ address ] = { DD_SOURCES:[ "cb marker", ] }

    # combine both matches
    match.extend(cb_match)
    match.extend(addr_match)

    return match

def unify_miners(miner1,
                 miner2,
                 miners,
                 firstUsed_new=0,
                 lastUsed_new=0):
    """ Merge the first miner into the second

    If firstUsed and lastUsed values are provided, the old miner is kept in the miners JSON file,
    and it is assumed that the provided firstUsed is the point in time where miner1 was unified with miner2.

    miner1.lastUsed = miner2.firstUsed
    """
    if miner1 not in miners.keys() or miner2 not in miners.keys():
        raise InvalidMinerData("At least one of the miners not in miners JSON",miner1,miner2,miners)
    m1 = miners[ miner1 ]
    for addr in m1[ D_ADDRESSES ].keys():
        currencies = list()
        firstUsed = 0
        lastUsed = 0
        if DD_CURRENCIES in m1[ D_ADDRESSES ][ addr ].keys():
            currencies = m1[ D_ADDRESSES ][ addr ][ DD_CURRENCIES ]
        if DD_FIRSTUSED in m1[ D_ADDRESSES ][ addr ].keys() and firstUsed_new == 0:
            firstUsed = m1[ D_ADDRESSES ][ addr ][ DD_FIRSTUSED ]
        elif firstUsed_new > 0:
            # if firstUsed_new of m2 has been given, it corresponds to the lastUsed of m1
            firstUsed = firstUsed_new
            m1[ D_ADDRESSES ][ addr ][ DD_LASTUSED ] = firstUsed_new-1
        if DD_LASTUSED in m1[ D_ADDRESSES ][ addr ].keys() and firstUsed_new == 0:
            # only take old lastUsed value if no firstUsed_new has been provided
            # if firstUsed_new is non zero lastUsed is zero if not explicitly specified
            lastUsed = m1[ D_ADDRESSES ][ addr ][ DD_LASTUSED ]
        elif lastUsed_new > 0:
            # if lastUsed_new of m2 has been given, the address was depricated at some point
            lastUsed = lastUsed_new
        if DD_SOURCES in m1[ D_ADDRESSES ][ addr ].keys():
            sources = m1[ D_ADDRESSES ][ addr ][ DD_SOURCES ]
            for src in sources:
                add_addr(miner2,miners,addr,src,currencies,firstUsed,lastUsed)
        else:
            add_addr(miner2,miners,addr,list(),currencies,firstUsed,lastUsed)

    for marker in m1[ D_MARKERS ].keys():
        currencies = list()
        firstUsed = 0
        lastUsed = 0
        if DD_CURRENCIES  in m1[ D_MARKERS ][ marker ].keys():
            currencies = m1[ D_MARKERS ][ marker ][ DD_CURRENCIES ]
        if DD_FIRSTUSED in m1[ D_MARKERS ][ marker ].keys() and firstUsed_new == 0:
            firstUsed = m1[ D_MARKERS ][ marker ][ DD_FIRSTUSED ]
        elif firstUsed_new > 0:
            firstUsed = firstUsed_new
            m1[ D_MARKERS ][ marker ][ DD_LASTUSED ] = firstUsed_new-1
        if DD_LASTUSED in m1[ D_MARKERS ][ marker ].keys() and firstUsed_new == 0:
            lastUsed = m1[ D_MARKERS ][ marker ][ DD_LASTUSED ]
        elif lastUsed_new > 0:
            lastUsed = lastUsed_new
        if DD_SOURCES in m1[ D_MARKERS ][ marker ].keys():
            sources = m1[ D_MARKERS ][ marker ][ DD_SOURCES ]
            for src in sources:
                add_marker(miner2,miners,marker,src,currencies,firstUsed,lastUsed)
        else:
            add_marker(miner2,miners,marker,list(),currencies,firstUsed,lastUsed)

    for name in m1[ D_NAMES ].keys():
        currencies = list()
        firstUsed = 0
        lastUsed = 0
        url = ""
        fullname = ""
        if DD_CURRENCIES  in m1[ D_NAMES ][ name ].keys():
            currencies = m1[ D_NAMES ][ name ][ DD_CURRENCIES ]
        if DD_FIRSTUSED  in m1[ D_NAMES ][ name ].keys() and firstUsed_new == 0:
            firstUsed = m1[ D_NAMES ][ name ][ DD_FIRSTUSED ]
        elif firstUsed_new > 0:
            firstUsed = firstUsed_new
            m1[ D_NAMES ][ name ][ DD_LASTUSED ] = firstUsed_new-1
        if DD_LASTUSED in m1[ D_NAMES ][ name ].keys() and firstUsed_new == 0:
            lastUsed = m1[ D_NAMES ][ name ][ DD_LASTUSED ]
        elif lastUsed_new > 0:
            lastUsed = lastUsed_new
        if DD_URL  in m1[ D_NAMES ][ name ].keys():
            url = m1[ D_NAMES ][ name ][ DD_URL ]
        if DD_FULLNAME  in m1[ D_NAMES ][ name ].keys():
            fullname = m1[ D_NAMES ][ name ][ DD_FULLNAME ]
        if DD_SOURCES  in m1[ D_NAMES ][ name ].keys():
            sources = m1[ D_NAMES ][ name ][ DD_SOURCES ]
            for src in sources:
                add_name(miner2,miners,name,src,currencies,fullname,url,firstUsed,lastUsed)
        else:
            add_name(miner2,miners,name,list(),currencies,fullname,url,firstUsed,lastUsed)

    if firstUsed_new == 0:
        # remove miner one for this instance of miners
        # if now period for miner1 is defined by firstUsed
        #del miners[ miner1 ]
        miners.pop( miner1 )
    return miners

def get_aligned_blkidx(startTime=None,
                       endTime=None,
                       startBlkidx=None,
                       endBlkidx=None,
                       backTime=None,
                       currentBlkheight=None):
    """ Get difficulty adjustment aligned block number (index) values for block periods or time periods

    If interval should be defined by time, startTime is mandatory, endTime is optional and set to utcnow if omitted.

    If interval should be defined by block index, startBlkidx is mandatory, endBlkidx is optonalö and set to most recent block that is aligned with the difficulty period

    If backTime is given, the time is subtracted from utcnow() and aligned with difficulty periods.

    Parameters
    ----------
    startTime: datetime
        Start time of interval

    endTime: datetime
        End time of interval - if omitted set to utcnow()

    startBlkidx: int
        Some block index to start from, gets aligned with closest difficulty period

    endBlkidx: int
        Some block index to stop, gets aligend with closest difficulty period

    backTime: datetime
        Time that gets subtracted form utcnow() and aligned with closest difficutly period

    currentBlkheight: int
        Override CURRENT_BLOCKHEIGHT for testing

    Returns
    -------
    tuple
        Tuple of two int values (first,last) which are blocks indices that are aligned to difficulty periods

    """
    last_aligned_blkidx = 0
    first_aligned_blkidx = 0

    if not currentBlkheight:
        currentBlkheight = CURRENT_BLOCKHEIGHT
    genesis_dt = datetime.datetime(2009, 1, 3, 18, 15, 5) # genesis timestamp

    while ( last_aligned_blkidx + DIFFICULTY_PERIOD ) < currentBlkheight:
        last_aligned_blkidx += DIFFICULTY_PERIOD

    if startBlkidx:
        if endBlkidx:
            while ( last_aligned_blkidx + DIFFICULTY_PERIOD ) < endblkidx and ( last_aligned_blkidx < currentBlkheight) :
                last_aligned_blkidx += DIFFICULTY_PERIOD
        delta_blkcount = last_aligned_blkidx - startBlkidx
        first_aligned_blkidx = last_aligned_blkidx - delta_blkcount - ( last_aligned_blkidx - delta_blkcount ) % DIFFICULTY_PERIOD

    if startTime:
        start_delta = startTime - genesis_dt
        start_blkidx = int( int( start_delta.total_seconds() ) // 60 / 9.5 )
        while ( first_aligned_blkidx + DIFFICULTY_PERIOD ) < start_blkidx and ( first_aligned_blkidx < currentBlkheight) :
            first_aligned_blkidx += DIFFICULTY_PERIOD

        if endTime:
            end_delta = endTime - genesis_dt
            end_blkidx = int( int( end_delta.total_seconds() ) // 60 / 9.5 )
            while ( last_aligned_blkidx + DIFFICULTY_PERIOD ) < end_blkidx and ( last_aligned_blkidx < currentBlkheight) :
                last_aligned_blkidx += DIFFICULTY_PERIOD

    if backTime:
        dateNow = datetime.datetime.utcnow()

        delta_blkcount = backTime.days*BLOCK_DAY
        first_aligned_blkidx = last_aligned_blkidx - delta_blkcount - (last_aligned_blkidx - delta_blkcount) % DIFFICULTY_PERIOD

        #print("Target Date    : ",dateNow - backTime)
        #print("TimeDelta      : ",backTime)
        #print("delta_blkcount : ",delta_blkcount)
        #print("backTime.days  : ",backTime.days)

    assert ( last_aligned_blkidx < currentBlkheight )
    assert ( first_aligned_blkidx < last_aligned_blkidx )
    assert ( first_aligned_blkidx + last_aligned_blkidx ) % DIFFICULTY_PERIOD == 0
    return ( first_aligned_blkidx, last_aligned_blkidx )


def attribute_blocks(blocks,
                     miners_dict,
                     addr_attr,
                     marker_attr,
                     both_attr,
                     source,
                     override=False,
                     update=False):
    """ Attribute given blocks based on given miners_dict json

    Takes names for the different attribution per address, marker and both as well as a source
    from where the miners_dict information comes from. Overrides existing attributions with given
    names if override flag is set.
    Returns tuple of (blocks,miners_dict,conflicts) and does change miners_dict in the process.
    """
    i = 0
    conflicts = list()
    conflicts.clear()

    for blknum in blocks:
        match = list()
        addr_match = list()
        cb_match = list()

        try:
            # first always test if not already attributed
            if ( addr_attr not in blocks[ blknum ][ D_ATTRIBUTIONS ].keys() ) or override:
                # match address
                if len( blocks[ blknum ][ D_ADDRESSES ] ) == 1:
                    # only match if there is just one output address in the coinbase
                    address = blocks[ blknum ][ D_ADDRESSES ][0]
                    match = match_address_to_miner( address, miners_dict, strict=False, blknum=int(blknum) )

                    if len( match ) >= 1:
                        # if multiple coinbase markers match we can get more than one match
                        matched_miners = defaultdict(list)
                        for ma in match:
                            matched_miners[ ma[0] ].append( ma[1] )
                        j = 0
                        attr = ""
                        for mi in matched_miners:
                            blocks[ blknum ][ D_ATTRIBUTIONS ][ addr_attr + attr ] = { DDD_MINER:mi,
                                                                                               "matches":matched_miners[mi],
                                                                                               DDD_SRC:source }
                            j += 1
                            attr = str(j)

            if ( marker_attr not in blocks[ blknum ][ D_ATTRIBUTIONS ].keys() ) or override:
                # match coinbase
                coinbase = blocks[ blknum ][ D_CB ]
                match = match_coinbase_to_miner( coinbase, miners_dict, strict=False, blknum=int(blknum) )

                if len( match ) >= 1:
                    # if multiple coinbase markers match we can get more than one match
                    matched_miners = defaultdict(list)
                    for ma in match:
                        matched_miners[ ma[0] ].append( ma[1] )
                    j = 0
                    attr = ""
                    for mi in matched_miners:
                        blocks[ blknum ][ D_ATTRIBUTIONS ][ marker_attr + attr ] = { DDD_MINER:mi,
                                                                                     "matches":matched_miners[mi],
                                                                                     DDD_SRC:source }
                        j += 1
                        attr = str(j)

            if ( both_attr not in blocks[ blknum ][ D_ATTRIBUTIONS ].keys() ) or override:
                # match both and update miners
                coinbase = blocks[ blknum ][ D_CB ]
                if len( blocks[ blknum ][ D_ADDRESSES ] ) == 1:
                    address = blocks[ blknum ][ D_ADDRESSES ][0]
                    match = match_miner(miners_dict,address,coinbase,update=update, blknum=int(blknum) )
                else:
                    match = match_miner(miners=miners_dict,coinbase=coinbase, blknum=int(blknum) )

                if len( match ) > 0:
                    # There could be more than one marker of the same pool that matches simultaniously
                    matches = list()
                    #print(match)
                    for m in match:
                        matches.append( m[1] )
                    blocks[ blknum ][ D_ATTRIBUTIONS ][ both_attr ] = { DDD_MINER:match[0][0],
                                                                        "matches":matches,
                                                                        DDD_SRC:source }
        except ConflictingMinerData as e:
            print()
            print("Message    = ",e.message)
            print("Blockheight= ",blknum)
            print("Miner1     = ",e.miner1)
            print("Miner2     = ",e.miner2)
            print("Coinbase   = ",e.coinbase)
            print("CoinbaseStr= ",repr(binascii.unhexlify(e.coinbase)))
            print("Addesses   = ",e.address)
            print("addr_match = ",e.addr_match)
            print("cb_match   = ",e.cb_match)
            conflicts.append( { "message":e.message,
                                DDD_MINER + "1":e.miner1,
                                DDD_MINER + "2":e.miner2,
                                D_CB + "1":e.coinbase,
                                "address": e.address,
                                "addr_match": e.addr_match,
                                "cb_match": e.cb_match,
                                DDD_SRC:source } )

        # progress bar
        i+=1
        if i % 1000 == 0:
            print(i,end="\r")
            sys.stdout.flush()
    return (blocks,miners_dict,conflicts)

def check_for_miner_addresses_from_markers(miners):
    """ check how many miner addresses have been added based on markers
    """
    i = 0
    for miner in miners:
        for addr in miners[ miner ][ D_ADDRESSES ]:
            if DD_SOURCES in miners[ miner ][ D_ADDRESSES ][ addr ]:
                if "cb marker" in miners[ miner ][ D_ADDRESSES ][ addr ][ DD_SOURCES ]:
                    #print("miner: ",miner," addr: ", addr)
                    i += 1
            #else:
                #print(miners[ miner ])
                #break
    return i


def check_for_obvious_address_collisions(miners,return_conflicts=False):
    """ check miners for obvious address collisions
    i.e., addresses occuring in multiple miners
    """
    addr_collision = False
    address_conflicts = Counter()
    address_conflicts_list = list()

    for m in miners:
        for a in miners[ m ][ D_ADDRESSES ].keys():
            address_conflicts[ a ] += 1

    for at in address_conflicts.most_common():
        if at[1] > 1:
            address_conflicts_list.append( at[0] )
            print(at[0],":",at[1])
            addr_collision = True

    if return_conflicts:
       return address_conflicts_list
    else:
       return addr_collision
