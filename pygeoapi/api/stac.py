# =================================================================

# Authors: Tom Kralidis <tomkralidis@gmail.com>
#          Francesco Bartoli <xbartolone@gmail.com>
#          Sander Schaminee <sander.schaminee@geocat.net>
#          John A Stevenson <jostev@bgs.ac.uk>
#          Colin Blackburn <colb@bgs.ac.uk>
#          Ricardo Garcia Silva <ricardo.garcia.silva@geobeyond.it>
#          Bernhard Mallinger <bernhard.mallinger@eox.at>
#
# Copyright (c) 2024 Tom Kralidis
# Copyright (c) 2022 Francesco Bartoli
# Copyright (c) 2022 John A Stevenson and Colin Blackburn
# Copyright (c) 2023 Ricardo Garcia Silva
# Copyright (c) 2024 Bernhard Mallinger
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# =================================================================


from http import HTTPStatus
import logging
from typing import Tuple

from pygeoapi import l10n
from pygeoapi.plugin import load_plugin

from pygeoapi.provider.base import (
    ProviderConnectionError, ProviderNotFoundError
)
from pygeoapi.util import (
    get_provider_by_type, to_json, filter_dict_by_key_value,
    render_j2_template
)

from . import APIRequest, API, FORMAT_TYPES, F_JSON, F_HTML
# STAC API
from itertools import repeat
import operator
import json
from collections import Counter
import shapely
from shapely.geometry import shape

LOGGER = logging.getLogger(__name__)


CONFORMANCE_CLASSES = [
    "https://api.stacspec.org/v1.0.0/collections",
    "https://api.stacspec.org/v1.0.0/core",
    "https://api.stacspec.org/v1.0.0/ogcapi-features",
    "https://api.stacspec.org/v1.0.0/item-search",
    "https://api.stacspec.org/v1.0.0/item-search#sort",
    "https://api.stacspec.org/v1.0.0/collections"
]


# TODO: no tests for this?
def get_stac_root(api: API, request: APIRequest) -> Tuple[dict, int, str]:
    """
    Provide STAC root page

    :param request: APIRequest instance with query params

    :returns: tuple of headers, status code, content
    """
    headers = request.get_response_headers(**api.api_headers)

    id_ = 'pygeoapi-stac'
    stac_version = '1.0.0'
    stac_url = f'{api.base_url}/stac'

    content = {
        'id': id_,
        'type': 'Catalog',
        'stac_version': stac_version,
        'title': l10n.translate(
            api.config['metadata']['identification']['title'],
            request.locale),
        'description': l10n.translate(
            api.config['metadata']['identification']['description'],
            request.locale),
        'conformsTo': CONFORMANCE_CLASSES,
        'links': []
    }

    stac_collections = filter_dict_by_key_value(api.config['resources'],
                                                'type', 'stac-collection')

    for key, value in stac_collections.items():
        content['links'].append({
            'rel': 'child',
            'href': f'{stac_url}/{key}?f={F_JSON}',
            'type': FORMAT_TYPES[F_JSON]
        })
        # For application that doesn't need HTML return. ex. QGIS plugin
        if request.format == F_HTML:
            content['links'].append({
                'rel': 'child',
                'href': f'{stac_url}/{key}',
                'type': FORMAT_TYPES[F_HTML]
            })

    # Adding root, self, openapi service decs and STAC-API Search endpoint in the Catalog's links
    content['links'].append({'rel': 'root', 'href': f'{stac_url}', 'type': FORMAT_TYPES[F_JSON]})
    content['links'].append({'rel': 'self', 'href': f'{stac_url}', 'type': FORMAT_TYPES[F_JSON]})
    content['links'].append({"rel": "service-desc", "type": "application/vnd.oai.openapi+json;version=3.0", "href": f"{api.base_url}/openapi?f=json"})
    content['links'].append({"rel": "service-doc", "type": "text/html", "href": f"{api.base_url}/openapi?f=html"})
    content['links'].append({"rel": "search", "type": "application/geo+json", "title": "STAC search", "href": f"{stac_url}/search", "method": "GET"})

    if request.format == F_HTML:  # render
        content = render_j2_template(api.tpl_config,
                                     'stac/collection.html',
                                     content, request.locale)
        return headers, HTTPStatus.OK, content

    return headers, HTTPStatus.OK, to_json(content, api.pretty_print)


# TODO: no tests for this?
def get_stac_path(api: API, request: APIRequest,
                  path) -> Tuple[dict, int, str]:
    """
    Provide STAC resource path

    :param request: APIRequest instance with query params

    :returns: tuple of headers, status code, content
    """
    headers = request.get_response_headers(**api.api_headers)

    dataset = None
    LOGGER.debug(f'Path: {path}')
    dir_tokens = path.split('/')
    if dir_tokens:
        dataset = dir_tokens[0]

    stac_collections = filter_dict_by_key_value(api.config['resources'],
                                                'type', 'stac-collection')

    if dataset not in stac_collections:
        msg = 'Collection not found'
        return api.get_exception(HTTPStatus.NOT_FOUND, headers,
                                 request.format, 'NotFound', msg)

    LOGGER.debug('Loading provider')
    try:
        p = load_plugin('provider', get_provider_by_type(
            stac_collections[dataset]['providers'], 'stac'))
    except ProviderConnectionError:
        msg = 'connection error (check logs)'
        return api.get_exception(
            HTTPStatus.INTERNAL_SERVER_ERROR, headers,
            request.format, 'NoApplicableCode', msg)

    id_ = f'{dataset}-stac'
    stac_version = '1.0.0'

    content = {
        'id': id_,
        'type': 'Catalog',
        'stac_version': stac_version,
        'description': l10n.translate(
            stac_collections[dataset]['description'], request.locale),
        'links': []
    }
    try:
        stac_data = p.get_data_path(
            f'{api.base_url}/stac',
            path,
            path.replace(dataset, '', 1)
        )
    except ProviderNotFoundError:
        msg = 'resource not found'
        return api.get_exception(HTTPStatus.NOT_FOUND, headers,
                                 request.format, 'NotFound', msg)
    except Exception:
        msg = 'data query error'
        return api.get_exception(
            HTTPStatus.INTERNAL_SERVER_ERROR, headers,
            request.format, 'NoApplicableCode', msg)

    if isinstance(stac_data, dict):
        content.update(stac_data)
        if (len(list(stac_collections[dataset]['links'][0].keys())) > 0):
            content['links'].extend(stac_collections[dataset]['links'])

        if request.format == F_HTML:  # render
            content['path'] = path
            if 'assets' in content:  # item view
                if content['type'] == 'Collection':
                    content = render_j2_template(
                        api.tpl_config,
                        'stac/collection_base.html',
                        content,
                        request.locale
                    )
                elif content['type'] == 'Feature':
                    content = render_j2_template(
                        api.tpl_config,
                        'stac/item.html',
                        content,
                        request.locale
                    )
                else:
                    msg = f'Unknown STAC type {content.type}'
                    return api.get_exception(
                        HTTPStatus.INTERNAL_SERVER_ERROR,
                        headers,
                        request.format,
                        'NoApplicableCode',
                        msg)
            else:
                content = render_j2_template(api.tpl_config,
                                             'stac/catalog.html',
                                             content, request.locale)

            return headers, HTTPStatus.OK, content

        return headers, HTTPStatus.OK, to_json(content, api.pretty_print)

    else:  # send back file
        headers.pop('Content-Type', None)
        return headers, HTTPStatus.OK, stac_data


def get_stac_collections(api: API, request: APIRequest, path) -> Tuple[dict, int, str]:
    """
    Provide STAC API - Collections endpoint implementation (/stac/collections)

    :param request: APIRequest instance with query params

    :returns: tuple of headers, status code, content

    :remark: This function is copied and modified from get_stac_path to avoid changes in the original.
    """

    if not request.is_valid():
        return api.get_format_exception(request)
    headers = request.get_response_headers(**api.api_headers)

    datasets = None
    LOGGER.debug(f'Path: {path.split("/")} , Request format : {request.format}')
    dir_tokens = path.split('/')  # ex : /collections/{collection_id} or /collections
    # if dir_tokens:
    # if (dir_tokens[0] != 'collections'):
    if (len(dir_tokens[0]) == 2):  # get specific collection by id
        datasets = [dir_tokens[-1]]
    else:
        # No collection id is specified. get all collections
        datasets = filter_dict_by_key_value(api.config['resources'], 'type', 'stac-collection')
        datasets = list(datasets.keys())

    stac_collections = filter_dict_by_key_value(api.config['resources'],
                                                'type', 'stac-collection')
    contents = []
    # get individual collection stac json
    for dataset in datasets:
        if dataset not in stac_collections:
            msg = 'Collection not found'
            return api.get_exception(HTTPStatus.NOT_FOUND, headers,
                                     request.format, 'NotFound', msg)

        LOGGER.debug('Loading provider')
        try:
            p = load_plugin('provider', get_provider_by_type(
                stac_collections[dataset]['providers'], 'stac'))
        except ProviderConnectionError as err:
            LOGGER.error(err)
            msg = 'connection error (check logs)'
            return api.get_exception(
                HTTPStatus.INTERNAL_SERVER_ERROR, headers,
                request.format, 'NoApplicableCode', msg)

        id_ = f'{dataset}-stac'
        stac_version = '1.0.0'

        content = {
            'id': id_,
            'type': 'Catalog',
            'stac_version': stac_version,
            'description': l10n.translate(
                stac_collections[dataset]['description'], request.locale),
            'links': []
        }
        try:
            stac_data = p.get_data_path(
                f'{api.base_url}/stac',
                dataset,
                ''
            )
        except ProviderNotFoundError as err:
            LOGGER.error(err)
            msg = 'resource not found'
            return api.get_exception(HTTPStatus.NOT_FOUND, headers,
                                     request.format, 'NotFound', msg)
        except Exception as err:
            LOGGER.error(err)
            msg = 'data query error'
            return api.get_exception(
                HTTPStatus.INTERNAL_SERVER_ERROR, headers,
                request.format, 'NoApplicableCode', msg)

        if isinstance(stac_data, dict):
            content.update(stac_data)
            if (content['type'] == 'Collection' and len(dir_tokens) > 1):
                content['title'] = f'{dataset}/{content["title"]}'
            # LOGGER.debug(f'stac_collections : {stac_collections[dataset]["links"]}')
            if (len(list(stac_collections[dataset]['links'][0].keys())) > 0):
                content['links'].extend(stac_collections[dataset]['links'])
            # LOGGER.debug(content['links'])
            if content['type'] == 'Feature':
                try:
                    del content['assets']['default']
                except KeyError:
                    pass
        contents.append(content)
    if (len(contents) == 1):
        # for request with collection_id, no need to enclose the content array inside collections dict
        return headers, HTTPStatus.OK, to_json(contents[0], api.pretty_print)
    else:
        contents = {'collections': contents}
        return headers, HTTPStatus.OK, to_json(contents, api.pretty_print)


def get_stac_search(api: API, request: APIRequest, method) -> Tuple[dict, int, str]:

    """
    Provide STAC API - Search endpoint implementation (/stac/search)

    :param request: APIRequest instance with query params

    :returns: tuple of headers, status code, content

    :remark: The search api will start from collection level. If there is no collection specify in the query,
             it will get the whole list from get_stac_root. Then it will retrieve all items by _recursiveSearchItems.
             The result item set form a superset for filtering. The superset will then apply to each search criteria and produce
             a mask array to indicate which items are matching the criteria. After all search criterias are done, those mask arrays
             are aggregated by logical OR to construct the final return.
    """
    # query_opr = {'eq': operator.eq, 'neq': operator.ne, 'lt': operator.lt, 'lte': operator.le,
    #             'gt': operator.gt, 'gte': operator.ge, 'startswith': None, 'endswith': None, 'contain': None, 'in': None}

    def _recursiveSearchItems(request, result):
        result = [r['href'] for r in result if (r['rel'] != 'root' and r['rel'] != 'self' and r['rel'] != 'parent')]
        result = [r.split('stac')[-1][1:] for r in result]
        result = list(map(get_stac_path, repeat(api), repeat(request), result))
        result = [r[2] for r in result]
        result = list(map(json.loads, result))
        items = [r for r in result if (r['type'] == 'Feature')]
        result = [r['links'] for r in result if (r['type'] != 'Feature')]
        recursive = []
        if (len(result) > 0):
            for r in result:
                for obj in r:
                    if (obj['rel'] == 'chlid' or obj['rel'] == 'item'):
                        recursive.append(obj)
            tmp = _recursiveSearchItems(request, recursive)
            items += tmp
        return items

    # For opertor
    # def _recursiveGetProperty(nodes, item):
    #    value = item[nodes[0]]
    #    for node in range(1, len(nodes)):
    #        value = value[nodes[i]]
    #    return value
    if not request.is_valid():
        return api.get_format_exception(request)
    headers = request.get_response_headers(**api.api_headers)
    headers['Content-Type'] = 'application/geo+json'
    # - b'{"limit": 10, "collections": ["eu_l8_EVI"], "sortby": [{"field": "collection", "direction": "asc"}]}'

    if (method == 'GET'):
        queries = request._args.to_dict()
        if (queries.get('ids')):
            ids = queries['ids'].split(',')
            queries['ids'] = ids
        if (queries.get('collections')):
            cols = queries['collections'].split(',')
            queries['collections'] = cols
        if (queries.get('bbox')):
            bbox = queries['bbox'].split(',')
            queries['bbox'] = bbox
    else:
        queries = json.loads(request._data.decode("utf-8"))
    LOGGER.debug(f'STAC search (Query Parameters) : {queries}')
    max_items = queries['max_items'] if (queries.get('max_items', '') != '') else -1
    sortby = queries['sortby'][0] if (queries.get('sortby', '') != '') else {'field': ''}
    if (queries.get('max_items') is not None):
        del queries['max_items']
    if (queries.get('sortby') is not None):
        del queries['sortby']
    if (queries.get('limit') is not None):
        del queries['limit']
    criteria = list(queries.keys())
    if ('bbox' in criteria and 'intersects' in criteria):
        return api.get_exception(HTTPStatus.BAD_REQUEST, headers,
                                 request.format, 'InvalidParameterValue',
                                 'Only one of either intersects or bbox may be specified')
    # Search : Start from collections level (biggest), then with others criteria for filtering.
    #          If the search doesn't comes with collections param , make one.
    if ('collections' not in criteria):
        LOGGER.debug('STAC search doesn\'t contain collections')
        root_result = get_stac_root(api, request)
        root_result = root_result[2]
        root_result = json.loads(root_result)['links']
        root_result = [r['href'] for r in root_result if (r['rel'] == 'child')]
        root_result = [r.split('/')[-1].split('?')[0] for r in root_result if (r.endswith('json'))]
        criteria.append('collections')
        queries['collections'] = root_result
    # Get items under each collections - super set
    collections = queries['collections']
    result = list(map(get_stac_path, repeat(api), repeat(request), collections))
    result = [r[2] for r in result]
    result = list(map(json.loads, result))
    LOGGER.debug(f'STAC search collections :{len(result)}')
    result = [r['links'] for r in result if (r.get('links', '') != '')]
    # result = ['/'.join(r.split('/')[-2:]) for r in result]
    result = map(_recursiveSearchItems, repeat(request), result)
    result = [obj for c in result for obj in c]
    # result = [o['href'] for r in result for o in r if (o['rel'] == 'item')]
    # result = ['/'.join(r.split('/')[-2:]) for r in result]
    LOGGER.debug(f'STAC search items:{len(result)}')
    # result = list(map(self.get_stac_path, repeat(request), result))
    # result = [r[2] for r in result]
    # result = list(map(json.loads, result))
    LOGGER.debug(f'STAC search collections item result :{len(result)}')
    # Filter's implememtation - create subset
    filter_idx = Counter()
    got_filter = False
    for filter_ in criteria:
        if (filter_ == 'ids'):
            got_filter = True
            ids = queries['ids']  # Assume WGS84
            LOGGER.debug(f'STAC search ids filter: {ids}')
            items_id = [obj['id'] for obj in result]
            LOGGER.debug(items_id)
            items_id = list(map(lambda x: (x in ids), items_id))
            find_idx = [i for i, x in enumerate(items_id) if (x)]
            LOGGER.debug(f'STAC search ids filter found : {find_idx}')
            filter_idx.update(find_idx)
        if (filter_ == 'bbox'):
            got_filter = True
            bbox = queries['bbox']  # Assume WGS84
            LOGGER.debug(f'STAC search bbox filter: {bbox}')
            bbox = shapely.geometry.box(*bbox)
            items_bbox = [obj['bbox'] for obj in result]
            items_bbox = list(map(lambda x: shapely.geometry.box(*x), items_bbox))
            items_bbox = list(map(lambda x: bbox.intersects(x), items_bbox))
            find_idx = [i for i, x in enumerate(items_bbox) if (x)]
            LOGGER.debug(f'STAC search bbox filter found : {find_idx}')
            filter_idx.update(find_idx)
        if (filter_ == 'intersects'):
            got_filter = True
            geojson = queries['intersects']
            LOGGER.debug(f'STAC search intersects filter: {geojson}')
            try:
                geoobj = shape(geojson)
            except Exception as e:
                return api.get_exception(HTTPStatus.BAD_REQUEST, headers,
                                         request.format, 'InvalidParameterValue',
                                         f'Error in converting geojson :{e}')
            items_geometry = [shape(obj['geometry']) for obj in result]
            items_geometry = list(map(lambda x: geoobj.intersects(x), items_geometry))
            find_idx = [i for i, x in enumerate(items_geometry) if (x)]
            LOGGER.debug(f'STAC search intersects filter found : {find_idx}')
            filter_idx.update(find_idx)
        # if (filter_ == 'query'):
        #    got_filter = True
        #    query = queries['query']  # Assume WGS84
        #    LOGGER.debug(f'STAC search query filter: {query}')
        #    properties = query.keys()
            # for p in properties:
            #    operators = query[p]
            #    nodes = p.split(';')
            #    try:
            #        items_value = list(map(lambda x: _recursiveGetProperty(nodes,x), result))
            #    except Exception as e:
            #        return self.get_exception(HTTPStatus.BAD_REQUEST, headers,
            #                                  request.format, 'InvalidParameterValue',
            #                                  f'Error in get property: {e}')
            #    opr_results = []
            #    try:
            #        for op in operators:
            #            opk = op.keys()
            #            if (query_opr.get(opk) != None):
            #                opr_results.append(list(map(lambda x: query_opr[opk](operators[opk],x),items_value)))
            #            elif ( opk.lower() in ['startswith', 'endswith', 'contain', 'in'] ):
            #                if (opk.lower() == 'startswith'):
            #                    opr_results.append(list(map(lambda x: x.startswith(operators[opk]),items_value)))
            #                if (opk.lower() == 'endswith'):
            #                    opr_results.append(list(map(lambda x: x.endswith(operators[opk]),items_value)))
            #                if (opk.lower() == 'contain'):
            #                    idx = list(map(lambda x: x.find(operators[opk]) ,items_value))
            #                    opr_results.append(np.where(idx!=-1,1,0))
            #                if (opk.lower() == 'in'):
            #                    opr_results.append(list(map(lambda x: (x in operators[opk]),items_value)))
            #            else:
            #                raise Exception('Operator not found')
            #    except Exception as e:
            #        return self.get_exception(HTTPStatus.BAD_REQUEST, headers,
            #                                  request.format, 'InvalidParameterValue',
            #                                  f'Error in evaluating operator : {e}')
            #    opr_results = np.array(opr_results,dtype=bool)
            #    opr_results = np.all(opr_results,axis=0)
            #    find_idx =  [ i for i,x in enumerate(opr_results) if (x) ]
            #   filter_idx.update(find_idx)

    find_idx = list(filter_idx.keys()) if (got_filter is True) else list(range(len(result)))
    result = [r for i, r in enumerate(result) if (i in find_idx)]
    result = sorted(result, key=lambda k: k.get(sortby['field'], ''))
    # for r in result:
    #    if (r['assets']['image']['type'] == asset_cogtype):
    #        try:
    #            s=r['assets']['image']['href']
    #            s=urllib.parse.unquote(s)
    #            s=s.split('?')[0]
    #            r['assets']['image']['href']="/".join(s.split('/')[7:8]+s.split('/')[9:])
    #        except KeyError:
    #            pass
    LOGGER.debug(f'STAC search filtered results : {len(result)}')
    # "context": { "returned":len(result), "limit":"0", "matched":len(find_idx) }
    max_items = len(result) if (max_items == -1) else max_items
    result = {"type": "FeatureCollection", "features": result[:max_items]}
    return headers, HTTPStatus.OK, to_json(result, api.pretty_print)

def get_oas_30(cfg: dict, locale: str) -> tuple[list[dict[str, str]], dict[str, dict]]:  # noqa
    """
    Get OpenAPI fragments

    :param cfg: `dict` of configuration
    :param locale: `str` of locale

    :returns: `tuple` of `list` of tag objects, and `dict` of path objects
    """

    LOGGER.debug('setting up STAC')
    stac_collections = filter_dict_by_key_value(cfg['resources'],
                                                'type', 'stac-collection')
    paths = {}
    if stac_collections:
        paths['/stac'] = {
            'get': {
                'summary': 'SpatioTemporal Asset Catalog',
                'description': 'SpatioTemporal Asset Catalog',
                'tags': ['stac'],
                'operationId': 'getStacCatalog',
                'parameters': [],
                'responses': {
                    '200': {'$ref': '#/components/responses/200'},
                    'default': {'$ref': '#/components/responses/default'}
                }
            }
        }
    return [{'name': 'stac'}], {'paths': paths}
