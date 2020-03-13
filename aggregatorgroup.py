#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
/***************************************************************************
Name                 : Class Aggregator Group
Description          : Aggregate neighbour polygon
                       -------------------
Begin                : 2020-1-29
Copyright            : (C) 2019 by IBAMA
email                : motta dot luiz at gmail.com

Update: 2020-01-29

 ***************************************************************************/

/***************************************************************************
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License as published by  *
 *   the Free Software Foundation; either version 2 of the License, or     *
 *   (at your option) any later version.                                   *
 *                                                                         *
 ***************************************************************************/
"""

import os, sys
from datetime import datetime
from dateutil.relativedelta import relativedelta
import argparse
from enum import Enum

try:
    from osgeo import ogr, osr
except ImportError:
    import ogr, osr

class ItemInvalidUnion():
    @staticmethod
    def getFields():
        return [
            { 'name': 'fid', 'type': ogr.OFTInteger },
            { 'name': 'type_fid', 'type': ogr.OFTString, 'width': 50 },
            { 'name': 'message', 'type': ogr.OFTString, 'width': 200 },
            { 'name': 'type_geometry', 'type': ogr.OFTString, 'width': 50 }
        ]

    @staticmethod
    def getNameGeometry():
        return 'geometry'

    def __init__(self, fid, type_fid, message, geometry):
        self.fid, self.type_fid, self.message, self.geom = fid, type_fid, message, geometry

    def getItem(self):
        return {
            'fid': self.fid,
            'type_fid': self.type_fid,
            'message': self.message,
            'type_geometry': self.geom.GetGeometryName(),
            'geometry': self.geom
        }

class AggregatorParams():
    # setAlert
    dsAlert = None # Memory
    layerAlert = None

    field_fid = 'objectid'
    field_type = 'tipo'
    field_stage = 'estagio'
    field_date = 'data_imagem'

    sep_join = ','
    relMonth = relativedelta(months=6)
    buffer_meter = 15

    # setParams
    srs = None
    ctArea = None
    ctOrigin = None

    @staticmethod
    def setParams(layer):
        srs = layer.GetSpatialRef()
        wkt7390 = 'PROJCS["Brazil / Albers Equal Area Conic (WGS84)",GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.01745329251994328,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4326"]],PROJECTION["Albers_Conic_Equal_Area"],PARAMETER["longitude_of_center",-50.0],PARAMETER["standard_parallel_1",10.0],PARAMETER["standard_parallel_2",-40.0],PARAMETER["latitude_of_center",-25.0],UNIT["Meter",1.0]]'
        sr7390 = osr.SpatialReference()
        sr7390.ImportFromWkt( wkt7390 )
        AggregatorParams.srs = srs
        AggregatorParams.ctArea = osr.CreateCoordinateTransformation( srs, sr7390 )
        AggregatorParams.ctOrigin = osr.CreateCoordinateTransformation( sr7390, srs )

    @staticmethod
    def setAlert(ds, layer):
        AggregatorParams.dsAlert = ds
        AggregatorParams.layerAlert = layer

    @staticmethod
    def getBufferBoundBox( geometry):
        def createGeom(envelop):
            ( minX, maxX, minY, maxY ) = envelop
            ring = ogr.Geometry(ogr.wkbLinearRing)
            ring.AddPoint(minX, minY)
            ring.AddPoint(maxX, minY)
            ring.AddPoint(maxX, maxY)
            ring.AddPoint(minX, maxY)
            ring.AddPoint(minX, minY)
            geom = ogr.Geometry(ogr.wkbPolygon)
            geom.AddGeometry( ring )
            return geom
        
        bbox = createGeom( geometry.GetEnvelope() )
        bbox.Transform( AggregatorParams.ctArea )
        ( minX, maxX, minY, maxY ) = bbox.GetEnvelope()
        minX -= AggregatorParams.buffer_meter
        maxX += AggregatorParams.buffer_meter
        minY -= AggregatorParams.buffer_meter
        maxY += AggregatorParams.buffer_meter
        bbox = createGeom( ( minX, maxX, minY, maxY ) )
        bbox.Transform( AggregatorParams.ctOrigin )
        return bbox

    @staticmethod
    def getBuffer(geometry, clone=False):
        geom = geometry.Clone() if clone else geometry
        geom.Transform( AggregatorParams.ctArea )
        buff = geom.Buffer( AggregatorParams.buffer_meter )
        buff.Transform( AggregatorParams.ctOrigin )
        return buff

    @staticmethod
    def getAreaHa(geom):
        geom = geom.Clone()
        geom.Transform( AggregatorParams.ctArea )
        return geom.GetArea() / 10000

    @staticmethod
    def getItemFromFeature(feature):
        items = feature.items()
        data_imagem = datetime.strptime( items[AggregatorParams.field_date], '%Y/%m/%d %H:%M:%S')
        return {
            'fid_feature': feature.GetFID(),
            'fid_source': items[ AggregatorParams.field_fid ],
            'date': data_imagem.date(),
            'type': items[ AggregatorParams.field_type ],
            'stage': items[ AggregatorParams.field_stage ],
            'geometry': feature.GetGeometryRef().Clone()
        }

    @staticmethod
    def saveGroupItem(layer, item):
        defn = layer.GetLayerDefn()
        feat = ogr.Feature( defn )
        feat.SetGeometry( item['geometry'] )
        del item['geometry']
        for k in item:
            feat.SetField( k, item[ k ] )
        layer.CreateFeature( feat )
        feat = None

    @staticmethod
    def checkMultiPolygon(geomCheck ):
        def createMultiPolygon(polygons):
            geom = ogr.Geometry(ogr.wkbMultiPolygon)
            for g in polygons:
                geom.AddGeometry( g )
            return geom

        geomType = geomCheck.GetGeometryType()
        if geomType == ogr.wkbMultiPolygon:
            return { 'hasChange': False, 'hasInvalid': False }
        elif geomType == ogr.wkbPolygon:
            multiPolygon = createMultiPolygon( [ geomCheck ] )
        elif geomType == ogr.wkbGeometryCollection:
            polygons = []
            for id1 in range( geomCheck.GetGeometryCount() ):
                geomPart =  geomCheck.GetGeometryRef( id1 )
                if geomPart.GetGeometryType() == ogr.wkbPolygon:
                    polygons.append( geomPart )
                elif geomPart.GetGeometryType() == ogr.wkbMultiPolygon:
                    for id2 in geomPart.GetGeometryCount():
                        polygons.append( geomPart.GetGeometryRef( id2) )
            if len( polygons ) == 0:
                return { 'hasChange': False, 'hasInvalid': True }
            multiPolygon = createMultiPolygon( polygons )
        else:
            return { 'hasChange': False, 'hasInvalid': True }
        return { 'hasChange': True, 'hasInvalid': False, 'geometry': multiPolygon }

class ChainPolygons():
    type_fid_invalid = None
    invalidUnions = [] # ItemInvalidUnion.getItem

    def __init__(self, feature):
        self.seed = AggregatorParams.getItemFromFeature( feature )
        AggregatorParams.layerAlert.DeleteFeature( self.seed['fid_feature'] )

        self.itemsOutDate = [] # Temporaly, delete in 'search'
        self.itemsWithinDate = [] # Using for add features in Group
        self.dateIni = self.seed['date']
        self.dateEnd = self.seed['date']
        self.branches = [] # ChainPolygons

    def search(self, dateIni=None, dateEnd=None):
        def isWithinDate(date):
            return ( self.dateIni - AggregatorParams.relMonth ) <= date <= ( self.dateEnd + AggregatorParams.relMonth )

        def setDates(date):
            if date <  self.dateIni:
                self.dateIni = date
            if date >  self.dateEnd:
                self.dateEnd = date

        def checkItemsOutDate():
            if len( self.itemsOutDate ) == 0:
                del self.itemsOutDate
                return
            
            removeFids = []
            for id in range( len( self.itemsOutDate ) ):
                item = self.itemsOutDate[ id ]
                if isWithinDate( item['date'] ):
                    self.itemsWithinDate.append( item )
                    removeFids.append( id )
                    setDates( item['date'] )
                    if not AggregatorParams.layerAlert.GetFeature( item['fid_feature'] ) is None:
                        AggregatorParams.layerAlert.DeleteFeature( item['fid_feature'] )
                    
            if len( removeFids ) == 0:
                del self.itemsOutDate[:]
                del self.itemsOutDate
                return

            removeFids.reverse()
            for id in removeFids:
                del self.itemsOutDate[ id ]
            del removeFids[:]
            checkItemsOutDate()

        if not dateIni is None:
            self.dateIni = dateIni
        if not dateEnd is None:
            self.dateEnd = dateEnd

        bboxBuffer = AggregatorParams.getBufferBoundBox( self.seed['geometry'] )
        AggregatorParams.layerAlert.SetSpatialFilter( bboxBuffer )
        buffGeom = AggregatorParams.getBuffer( self.seed['geometry'], True )
        for feat in AggregatorParams.layerAlert:
            item = AggregatorParams.getItemFromFeature( feat )
            if buffGeom.Intersects( item['geometry'] ):
                if isWithinDate( item['date'] ):
                    self.branches.append( ChainPolygons( feat ) )
                    setDates( item['date'] )
                else:
                    self.itemsOutDate.append( item )

        AggregatorParams.layerAlert.SetSpatialFilter( None )
        for branch in self.branches:
            branch.search( self.dateIni, self.dateEnd )
            self.dateIni = branch.dateIni
            self.dateEnd = branch.dateEnd

        AggregatorParams.layerAlert.SetSpatialFilter( None )
        checkItemsOutDate()

    def initValues(self):
        date = self.seed['date']
        return {
            'areaHa': AggregatorParams.getAreaHa( self.seed['geometry'] ),
            'fids': [ self.seed['fid_source'] ],
            'dates': { 'ini': self.dateIni, 'end': self.dateEnd },
            'dates_ev': [ date ],
            'tipos': [ self.seed['type'] ],
            'estagios': [ self.seed['stage'] ],
            'union': self.seed['geometry']
        }

    def groupValues(self, value, branches):
        def addUniqueValues(item, value):
            items_values = [
                { 'item': 'date', 'value': 'dates_ev' },
                { 'item': 'type', 'value': 'tipos' },
                { 'item': 'stage', 'value': 'estagios' }
            ]
            for iv in items_values:
                if not item[ iv['item'] ] in value[ iv['value'] ]:
                    value[ iv['value'] ].append( item[ iv['item'] ] )

        def addUnion(item, value):
            union, msg = None, None
            try:
                union = value['union'].Union( item['geometry'] )
            except Exception as error:
                msg = "{}".format( error )
            if union is None or union.IsValid() == False:
                msg = msg if union is None else 'Union is not Valid'
                iiu = ItemInvalidUnion( item['fid_source'], self.type_fid_invalid, msg, item['geometry'] )
                self.invalidUnions.append( iiu.getItem() )
                return
            r = AggregatorParams.checkMultiPolygon( union )
            if r['hasChange']:
                union.Destroy()
                union = r['geometry']
            if r['hasInvalid']:
                msg = 'Missing polygon in Union'
                iiu = ItemInvalidUnion( item['fid_source'], self.type_fid_invalid, msg, item['geometry'] )
                self.invalidUnions.append(  iiu.getItem() )
                return
            value['union'].Destroy()
            value['union'] = union

        for branch in branches:
            value['fids'].append( branch.seed['fid_source'] )
            addUniqueValues( branch.seed, value )
            addUnion( branch.seed, value )
        
        for branch in branches:
            self.groupValues( value, branch.branches )

        if len( self.itemsWithinDate ) > 0:
            for item in self.itemsWithinDate:
                addUniqueValues( item, value )
                addUnion( item, value )

class AggregatorGroup():
    @staticmethod
    def init(tableAlert):
        ChainPolygons.type_fid_invalid = f"{AggregatorParams.field_fid} from '{tableAlert}'"
        ChainPolygons.invalidUnions.clear()

    @staticmethod
    def createGroups():
        def createGroup(idGroup, chainPolygons):
            value = chainPolygons.initValues()
            chainPolygons.groupValues( value, chainPolygons.branches )
            return {
                'id_group': idGroup,
                'n_events': len( value['dates_ev'] ),
                'ini_date': value['dates']['ini'].strftime("%Y-%m-%d"),
                'end_date': value['dates']['end'].strftime("%Y-%m-%d"),
                'ini_ha': value['areaHa'],
                'end_ha': AggregatorParams.getAreaHa( value['union'] ),
                'n_fids': len( value['fids'] ),
                'fids': AggregatorParams.sep_join.join( sorted( [ str(fid) for fid in  value['fids']  ] ) ),
                'dates_ev': AggregatorParams.sep_join.join( sorted( [ d.strftime("%Y-%m-%d") for d in value['dates_ev'] ] ) ),
                'tipos': AggregatorParams.sep_join.join( sorted( value['tipos'] ) ),
                'estagios': AggregatorParams.sep_join.join( sorted( value['estagios'] ) ),
                'geometry': value['union']
            }

        totalNewGroup = 0
        while(1):
            feat = AggregatorParams.layerAlert.GetNextFeature()
            if feat is None:
                break
            chainPolygons = ChainPolygons( feat )
            chainPolygons.search()
            totalNewGroup += 1
            group = createGroup( totalNewGroup, chainPolygons )
            yield group
