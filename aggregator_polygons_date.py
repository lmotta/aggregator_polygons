#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
/***************************************************************************
Name                 : Aggregator polygon date
Description          : Union neighbour polygon from SISCOM 'ibama.alerta' and create/update 'agregado.alert_aggregated'
Arguments            : Optional parameter -c (create) otherwise update

                       -------------------
Begin                : 2018-08-24
Copyright            : (C) 2018 by IBAMA
email                : motta dot luiz at gmail.com

Update: 2020-03-13

 ***************************************************************************/

/***************************************************************************
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License as published by  *
 *   the Free Software Foundation; either version 2 of the License, or     *
 *   (at your option) any later version.                                   *
 *                                                                         *
 ***************************************************************************/

Implementation: Generator and memory

"""

import os, sys
from datetime import datetime
import argparse
from enum import Enum

from aggregatorgroup import ItemInvalidUnion, AggregatorParams, ChainPolygons, AggregatorGroup

try:
    from osgeo import ogr, osr
except ImportError:
    import ogr, osr

class StatusProcess(Enum):
    PROCESSING = 'Processing...'
    SUCCESS = 'Success'

class AggregatorGroupPG():
    # setPostgres
    dsPG = None
    str_conn = None

    tableAlert = 'ibama.alerta'
    tableAgregated = 'agregado.alert_aggregated'
    meta_item_description = 'DESCRIPTION'
    field_carga = 'dt_carga'
    labelDatetime = 'Started processing:'
        
    dtInit = None

    @staticmethod
    def setPostgres(user, password, host, db):
        AggregatorGroupPG.str_conn = f"PG: host={host} dbname={db} user={user} password={password}"
        AggregatorGroupPG.dsPG = ogr.Open( AggregatorGroupPG.str_conn, update=1 )

    @staticmethod
    def openPostgres():
        AggregatorGroupPG.dsPG = ogr.Open( AggregatorGroupPG.str_conn, update=1 )

    @staticmethod
    def setProcessParams(printStatus, useFilterDatetime=False):
        def getSqlLayerAlert():
            def getLastUpdate():
                def getItemStarted(values):
                    for item in values:
                        if not item.find( AggregatorGroupPG.labelDatetime ) == -1:
                            return item
                    return None

                layer = AggregatorGroupPG.dsPG.GetLayerByName( AggregatorGroupPG.tableAgregated )
                if layer is None:
                    return { 'isOk': False, 'message': f"Missing '{AggregatorGroupPG.tableAgregated}'" }
                metadata = layer.GetMetadata()
                description = metadata[ AggregatorGroupPG.meta_item_description ]
                values = description.split('\n')
                item = getItemStarted( values )
                if item is None:
                    return { 'isOk': False, 'message': "Missing '{}' in comment from {}".format( AggregatorGroupPG.labelDatetime, AggregatorGroupPG.tableAgregated ) }
                idx = item.find( AggregatorGroupPG.labelDatetime ) + len( AggregatorGroupPG.labelDatetime ) + 1
                date_time = item[idx:]
                layer = None
                return { 'isOk': True, 'date_time': date_time }

            def getSql(date_time=None):
                args = (
                    AggregatorParams.field_fid,
                    AggregatorParams.field_type,
                    AggregatorParams.field_stage,
                    AggregatorParams.field_date,
                )
                s_select = "a.{}, a.{}, a.{}, a.{}, a.geom\n".format( *args )
                s_from = "{} AS a, cb.lim_pais_a AS l\n".format( AggregatorGroupPG.tableAlert )
                where_att = ["NOT a.data_imagem IS NULL", "NOT a.estagio IN ('FF+', 'CICATRIZ_DE_QUEIMADA', 'FF')" ]
                where_geom = ["ST_IsValid( a.geom )", "ST_Intersects( a.geom, l.geom )" ]
                if not date_time is None:
                    v = "a.{} > TO_TIMESTAMP('{}', 'YYYY-MM-DD HH24:MI:SS.US')".format( AggregatorGroupPG.field_carga, date_time )
                    where_att.append( v )
                s_where = "{} AND {}".format( ' AND '.join( where_att ), ' AND '.join( where_geom ) )
                return "SELECT {} FROM {} WHERE {}".format( s_select, s_from, s_where )

            if useFilterDatetime:
                r = getLastUpdate()
                if not r['isOk']:
                    return { 'isOk': False, 'message': r['message'] }
                sqlAlert = getSql( r['date_time'] )
            else:
                sqlAlert = getSql()
            try:
                layer = AggregatorGroupPG.dsPG.ExecuteSQL( sqlAlert )
            except Exception as error:
                return { 'isOk': False, 'message': error }
            if layer is None:
                msg = f"Fail get SQL for Alert: {sqlAlert}"
                return { 'isOk': False, 'message': msg }
            return { 'isOk': True, 'layer': layer }

        def createDsLayerMemoryAlert(layer):
            def createDS():
                memDS = 'memData'
                driver = ogr.GetDriverByName('MEMORY')
                ds = driver.CreateDataSource( memDS )
                driver.Open(memDS, 1 ) # Write access
                return ds

            memLayerName = 'alert'
            ds = createDS()
            layer = ds.CopyLayer( layer, memLayerName, ['OVERWRITE=YES'] )
            layer.ResetReading()

            return { 'ds': ds, 'layer': layer }

        # AggregatorParams: srs, ctArea and ctOrigin
        layer = AggregatorGroupPG.dsPG.GetLayerByName( AggregatorGroupPG.tableAlert )
        if layer is None:
            msg = f"Missing table '{AggregatorGroupPG.tableAlert}' in DB "
            return { 'isOk': False, 'message': msg }
        AggregatorParams.setParams( layer )
        layer = None

        # Get Layer from SQL alert
        r = getSqlLayerAlert()
        if not r['isOk']:
            return r
        msg = f"Copying alerts in memory... - {datetime.now()}"
        printStatus( msg)
        
        r = createDsLayerMemoryAlert( r['layer'] )
        AggregatorParams.setAlert( r['ds'], r['layer'] )

        msg = f"Copied { r['layer'].GetFeatureCount()} alerts in memory - {datetime.now()}"
        printStatus( msg, True )

        return { 'isOk': True }

    @staticmethod
    def getMetadata(statusProcess):
        args = (
            os.path.basename( __file__ ),
            str( datetime.now() ),
            AggregatorGroupPG.labelDatetime,
            str( AggregatorGroupPG.dtInit ),
            statusProcess.value
        )
        metadata = "Author: Luiz Motta\nCopyright: IBAMA\nScript: {}\nCreated/Updated: {}\n{} {}\nStatus: {}".format(*args)
        return metadata

    @staticmethod
    def createLayerPostgres(geom_type, name, fields):
        if not AggregatorGroupPG.dsPG.GetLayerByName( name ) is None:
            AggregatorGroupPG.dsPG.DeleteLayer( name )
        layer = AggregatorGroupPG.dsPG.CreateLayer( name, srs=AggregatorParams.srs, geom_type=geom_type, options=['OVERWRITE=YES'] )
        if layer is None:
            return { 'isOk': False, 'message': f"The table '{name}' not be created" }
        for item in fields:
            f = ogr.FieldDefn( item['name'], item['type'] )
            if 'width' in item:
                f.SetWidth( item[ 'width' ] )
            layer.CreateField( f )
        return { 'isOk': True, 'layer': layer }

    @staticmethod
    def createLayerInvalidUnion(errors):
        fields = ItemInvalidUnion.getFields()
        name = "{}_invalid_union".format( AggregatorGroupPG.tableAgregated )
        args = ( ogr.wkbUnknown, name, fields )
        r = AggregatorGroupPG.createLayerPostgres( *args )
        if not r['isOk']:
            return r
        layer = r['layer']
        defn = layer.GetLayerDefn()
        keys = [ f['name'] for f in fields ]
        nameGeom = ItemInvalidUnion.getNameGeometry()
        for item in errors:
            feat = ogr.Feature( defn )
            for k in keys:
                feat.SetField( k, item[ k ] )
            feat.SetGeometry( item[ nameGeom ] )
            layer.StartTransaction()
            layer.CreateFeature( feat )
            layer.CommitTransaction()
            feat = None
        layer.StartTransaction()
        layer.SetMetadataItem( AggregatorGroupPG.meta_item_description, AggregatorGroupPG.getMetadata( StatusProcess.SUCCESS ) )
        layer.CommitTransaction()

        return { 'isOk': True, 'table': name }
       
    @staticmethod
    def saveGroups(aggGroups, printStatus):
        def createMemoryLayerAggregator():
            # Data Source
            memDS = 'memDataAggregator'
            driver = ogr.GetDriverByName('MEMORY')
            ds = driver.CreateDataSource( memDS )
            driver.Open( memDS, 1 ) # Write access
            # Layer
            layer = ds.CreateLayer( AggregatorGroupPG.tableAgregated, srs=AggregatorParams.srs, geom_type=ogr.wkbMultiPolygon )
            fields = [
                { 'name': 'id_group', 'type': ogr.OFTInteger },
                { 'name': 'n_events', 'type': ogr.OFTInteger },
                { 'name': 'ini_date', 'type': ogr.OFTString, 'width': 10 },
                { 'name': 'end_date', 'type': ogr.OFTString, 'width': 10 },
                { 'name': 'ini_ha', 'type': ogr.OFTReal },
                { 'name': 'end_ha', 'type': ogr.OFTReal },
                { 'name': 'n_fids', 'type': ogr.OFTInteger },
                { 'name': 'fids', 'type': ogr.OFTString },
                { 'name': 'dates_ev', 'type': ogr.OFTString, 'width': 200 },
                { 'name': 'tipos', 'type': ogr.OFTString, 'width': 200 },
                { 'name': 'estagios', 'type': ogr.OFTString, 'width': 200 }
            ]
            for item in fields:
                f = ogr.FieldDefn( item['name'], item['type'] )
                if 'width' in item:
                    f.SetWidth( item[ 'width' ] )
                layer.CreateField( f )

            return { 'ds': ds, 'layer': layer }

        def copyLayer2PG(layer):
            try:
                if not AggregatorGroupPG.dsPG.GetLayerByName( AggregatorGroupPG.tableAgregated ) is None:
                    AggregatorGroupPG.dsPG.DeleteLayer( AggregatorGroupPG.tableAgregated )
            except Exception as error:
                msg = "\n*Open again DB, error: {}".format( error )
                printStatus( msg, True)
                AggregatorGroupPG.dsPG = None
                AggregatorGroupPG.openPostgres()
            args = ( layer, AggregatorGroupPG.tableAgregated, ['OVERWRITE=YES'] )
            return AggregatorGroupPG.dsPG.CopyLayer( *args )

        # MemoryLayerAggregator
        r =  createMemoryLayerAggregator()
        totalNewGroup = 0
        for item in aggGroups:
            totalNewGroup += 1
            if totalNewGroup % 1000 == 0:
                args = ( totalNewGroup, item['n_fids'], datetime.now() )
                msg = "Group {} ({} features)- {}...".format( *args )
                printStatus( msg )
            AggregatorParams.saveGroupItem( r['layer'], item )
        AggregatorParams.dsAlert = None # Use by aggGroups
        args = ( totalNewGroup, AggregatorGroupPG.tableAgregated, datetime.now() )
        msg = "Copying {} groups to DB '{}' - {}...".format( *args )
        printStatus( msg )
        layerGroup = copyLayer2PG( r['layer'] )
        r['layer'] = None
        r['ds'] = None
        status = StatusProcess.SUCCESS
        metadata = AggregatorGroupPG.getMetadata( status )
        value = f"{metadata}\nAdded {totalNewGroup} groups"
        try:
            layerGroup.SetMetadataItem( AggregatorGroupPG.meta_item_description, value )
        except Exception:
            AggregatorGroupPG.dsPG = None
            AggregatorGroupPG.openPostgres()
            layerGroup.SetMetadataItem( AggregatorGroupPG.meta_item_description, value )
        
        msg = "Copied {} groups to DB '{}' - {}...".format( *args )
        printStatus( msg )

        return { 'isOk': True, 'totalNewGroup': totalNewGroup }

    @staticmethod
    def updateGroups(printStatus):
        def getLayerAggregate():
            layer = AggregatorGroupPG.dsPG.GetLayerByName( AggregatorGroupPG.tableAgregated )
            if layer is None:
                return { 'isOk': False, 'message': "Missing layer '{}' in DB".format( AggregatorGroupPG.tableAgregated ) }
            return { 'isOk': True, 'layer': layer }

        def setGroup(group, totalDeleteGroup):
            def getDates(data):
                sFormat = '%Y-%m-%d'
                r = {}
                for d in ('ini_date', 'end_date'):
                    r[ d ] = datetime.strptime( data[ d ], sFormat )
                return r

            def isWithinDate(dates, datesFeat):
                return dates['ini_date'] >= ( datesFeat['ini_date'] - AggregatorParams.relMonth ) and dates['end_date'] <= ( datesFeat['end_date'] + AggregatorParams.relMonth )

            def unionGroup():
                def addUniqueValues():
                    keys = ('fids', 'dates_ev', 'tipos', 'estagios')
                    for k in keys:
                        l1 = group[ k ].split( AggregatorParams.sep_join )
                        l2 = feat[ k ].split( AggregatorParams.sep_join )
                        group[ k ] = AggregatorParams.sep_join.join( list( set( l1 + l2 ) ) )
                    group['n_fids'] = len( group['fids'].split( AggregatorParams.sep_join ) )
                    group['n_events'] = len( group['dates_ev'].split( AggregatorParams.sep_join ) )

                union, msg = None, None
                try:
                    union = group['geometry'].Union( geomFeat )
                except Exception as error:
                    msg = "{}".format( error )
                if union is None or union.IsValid() == False:
                    msg = msg if union is None else 'Union is not Valid'
                    iiu = ItemInvalidUnion( group['id_group'], type_fid, msg,  geomFeat )
                    ChainPolygons.invalidUnions.append( iiu.getItem() )
                    return
                r = AggregatorParams.checkMultiPolygon( union )
                if r['hasChange']:
                    union.Destroy()
                    union = r['geometry']
                if r['hasInvalid']:
                    msg = 'Missing polygon in Union'
                    iiu = ItemInvalidUnion( group['id_group'], type_fid, msg,  geomFeat )
                    ChainPolygons.invalidUnions.append( iiu.getItem() )
                    return
                group['geometry'].Destroy()
                group['geometry'] = union
                group['end_ha'] = AggregatorParams.getAreaHa( union )
                if group['ini_date'] > feat['ini_date']:
                    group['ini_date'] = feat['ini_date']
                    group['ini_ha'] = feat['ini_ha']
                if group['end_date'] < feat['end_date']:
                    group['end_date'] = feat['end_date']
                addUniqueValues() # 'fids', 'dates_ev', 'tipos', 'estagios', 'n_fids', 'n_events'

            bboxBuffer = AggregatorParams.getBufferBoundBox( group['geometry'] )
            layerGroup.SetSpatialFilter( bboxBuffer )
            if layerGroup.GetFeatureCount() == 0:
                layerGroup.SetSpatialFilter(None)
                return
            dates = getDates( group )
            buffGeom = AggregatorParams.getBuffer( group['geometry'], True )
            fidsLayer = []
            type_fid = f"id_group from '{AggregatorGroupPG.tableAgregated}'." # Invalid Union
            for feat in layerGroup:
                datesFeat = getDates( feat )
                if isWithinDate( dates, datesFeat ):
                    geomFeat = feat.GetGeometryRef()
                    if buffGeom.Intersects( geomFeat ):
                        unionGroup()
                        dates = getDates( group )
                        fidsLayer.append( feat.GetFID() )
            layerGroup.SetSpatialFilter(None)
            total = len( fidsLayer )
            if total > 0:
                for fid in fidsLayer:
                    layerGroup.DeleteFeature( fid )
                totalDeleteGroup['value'] += total
            
        r = getLayerAggregate()
        if not r['isOk']:
            return r
        layerGroup = r['layer']
        totalGroup = layerGroup.GetFeatureCount()
        totalDeleteGroup = { 'value': 0 }
        totalNewGroup = 0
        for item in AggregatorGroup.createGroups():
            totalNewGroup += 1
            if totalNewGroup % 1000 == 0:
                args = ( totalNewGroup, item['n_fids'], datetime.now() )
                msg = "Group {} ({} features)- {}...".format( *args )
                printStatus( msg )
            item['id_group'] = totalGroup + totalNewGroup
            setGroup( item, totalDeleteGroup )
            layerGroup.StartTransaction()
            AggregatorParams.saveGroupItem( layerGroup, item )
            layerGroup.CommitTransaction()
        AggregatorParams.dsAlert = None # Use by AggregatorGroup.createGroups()
        metadata = AggregatorGroupPG.getMetadata( StatusProcess.SUCCESS)
        value = f"{metadata}\nAdded {totalNewGroup} groups"
        layerGroup.StartTransaction()
        layerGroup.SetMetadataItem( AggregatorGroupPG.meta_item_description, value )
        layerGroup.CommitTransaction()
        args = ( AggregatorGroupPG.tableAgregated, totalNewGroup, datetime.now() )
        msg = "Saving '{}' in DB ( {} groups) - {}...".format( *args )
        printStatus( msg )
        return { 'isOk': True, 'totalNewGroup': totalNewGroup, 'totalGroup': totalGroup, 'totalDeleteGroup': totalDeleteGroup['value'] }

def run(quiet_status, create):
    def printStatus(status, newLine=False):
        if quiet_status and not newLine:
            return

        if newLine:
            ch = "\n"
        else:
            ch = ""
        sys.stdout.write( "\r{}".format( status.ljust(100) + ch ) )
        sys.stdout.flush()

    def messageDiffDateTime(dt1, dt2):
        diff = dt2 - dt1
        return "Days = {} hours = {}".format( diff.days, diff.seconds / 3600 )

    ogr.RegisterAll()
    ogr.UseExceptions()

    vars_env = ['USERPG', 'PWDPG']
    for v in vars_env:
        if not v in os.environ:
            msg = f"Missing '{v}' in OS enviroment"
            printStatus( msg, True )
            return 1

    args = ( os.environ['USERPG'], os.environ['PWDPG'], '10.1.25.143', 'siscom')
    AggregatorGroupPG.setPostgres( *args )
    if AggregatorGroupPG.dsPG is None:
        msg = f"Error connection database: host={args[2]} db={args[3]} user={args[0]}"
        printStatus( msg, True )
        return 1

    AggregatorGroupPG.dtInit = datetime.now()
    status  = 'Creation' if create else 'Update'
    msg = f"Started ({status} '{AggregatorGroupPG.tableAgregated}'): {AggregatorGroupPG.dtInit}"
    printStatus( msg, True )

    if create:
        r = AggregatorGroupPG.setProcessParams(printStatus)
    else:
        r = AggregatorGroupPG.setProcessParams(printStatus, useFilterDatetime=True)
    if not r['isOk']:
        printStatus( r['message'], True )
        return 1

    AggregatorGroup.init( AggregatorGroupPG.tableAlert )
    if create:
        aggGroups = AggregatorGroup.createGroups() # generator
        r = AggregatorGroupPG.saveGroups( aggGroups, printStatus )
        if not r['isOk']:
            printStatus( r['message'] )
            return 1
        dtEnd = datetime.now()
        msgDiff = messageDiffDateTime( AggregatorGroupPG.dtInit, dtEnd )
        args = ( AggregatorGroupPG.tableAgregated, r['totalNewGroup'], dtEnd, msgDiff )
        msg =  "Created '{}' in DB. Total Groups {} - {}({})".format( *args )
        printStatus( msg, True )
    else:
        r = AggregatorGroupPG.updateGroups( printStatus )
        if not r['isOk']:
            printStatus( r['message'] )
            return 1
        if r['totalNewGroup'] == 0:
            msg =  f"Missing new group '{AggregatorGroupPG.tableAgregated}' in DB."
        else:
            dtEnd = datetime.now()
            msgDiff = messageDiffDateTime( AggregatorGroupPG.dtInit, dtEnd )
            args = ( AggregatorGroupPG.tableAgregated, r['totalNewGroup'], r['totalDeleteGroup'], r['totalGroup'], dtEnd, msgDiff )
            msg =  "Updated '{}' in DB. Groups: New {}, Delete {}, Total {} - {}({})".format( *args ) 
        printStatus( msg, True )

    totalInvalidUnions = len( ChainPolygons.invalidUnions )
    if totalInvalidUnions > 0:
        r = AggregatorGroupPG.createLayerInvalidUnion( ChainPolygons.invalidUnions )
        msg =  "Created '{}' in DB".format( r['table'] ) if r['isOk'] else r['message']
        printStatus( msg, True )
        ChainPolygons.invalidUnions.clear()

    return 0

def main():
    parser = argparse.ArgumentParser(description='Update/Create aggregator polygon.' )
    parser.add_argument( '-q', '--quiet', action="store_false", help='Hides the processing status' )
    parser.add_argument( '-c', '--create', action="store_false", help='Create new aggregator' )

    args = parser.parse_args()
    return run( not args.quiet, not args.create )

if __name__ == "__main__":
    sys.exit( main() )
