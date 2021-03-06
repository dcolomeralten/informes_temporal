# -*- coding: utf-8 -*-
#
# Copyright (c) 2021, CloudBlue
# All rights reserved.
#

#
# Change 20210728 - David Colomer - Alten Spain for TTECH
# Customer TaxID is included in the report.
#

from connect.client import R

from reports.utils import convert_to_datetime, get_basic_value, get_value, today_str


# Customer TaxID included in Header

# CR20210831
#HEADERS = (
#    'Request ID','Created at','Last Change At','Customer ID','Customer Name',
#    'Customer Tax ID','Customer External ID','Asset ID','Asset External ID',
#    'Tech.Contact Name', 'TC Name Reason','Tech.Contact e-Mail','TC e-Mail Reason',
#    'Tech.Contact Phone','TC Phone Reason','Domain','Domain Reason'
#    )

HEADERS = (
    'Request ID','Created at','Last Change At','Customer ID','Customer Name',
    'Customer Tax ID','Customer External ID','Asset ID','Asset External ID',
    'TC Name Reason','TC e-Mail Reason',
    'TC Phone Reason','Domain','Domain Reason'
    )
# CR20210831 - FIN

# HEADERS = (
#    'Request ID', 
#    # CR20210820 - Fields removed.
#    # 'Request Type', 'Request Status',
#    # CR20210820 - END
#    'Created At', 'Last Change At',
#    # CR20210820 - Fields removed.
#    # 'Exported At',
#    # CR20210820 - END
#    'Customer ID', 'Customer Name', 
#    # CR20210728 - Field added
#    'Customer TaxID', 
#    # CR20210728 - END
#    'Customer External ID',
#    #'Tier 1 ID', 'Tier 1 Name', 'Tier 1 External ID',
#    #'Tier 2 ID', 'Tier 2 Name', 'Tier 2 External ID',
#    'Provider  ID', 'Provider Name', 'Vendor ID', 'Vendor Name',
#    #'Product ID', 'Product Name',
#    'Asset ID', 'Asset External ID', 'Transaction Type',
#    'Hub ID', 'Hub Name', 'Asset Status',
#)

def generate(
    client=None,
    parameters=None,
    progress_callback=None,
    renderer_type=None,
    extra_context_callback=None,
):
    requests = _get_requests(client, parameters)
    progress = 0
    total = requests.count()
    if renderer_type == 'csv':
        yield HEADERS
        progress += 1
        total += 1
        progress_callback(progress, total)

    for request in requests:
        connection = request['asset']['connection']
        if renderer_type == 'json':
            yield {
                HEADERS[idx].replace(' ', '_').lower(): value
                for idx, value in enumerate(_process_line(request, connection))
            }
        else:
            yield _process_line(request, connection)
        progress += 1
        progress_callback(progress, total)


def _get_requests(client, parameters):
    all_types = ['inquiring']
    all_connections = ['production']

    query = R()
    query &= R().created.ge(parameters['date']['after'])
    query &= R().created.le(parameters['date']['before'])

    if parameters.get('product') and parameters['product']['all'] is False:
        query &= R().asset.product.id.oneof(parameters['product']['choices'])
    if parameters.get('rr_type') and parameters['rr_type']['all'] is False:
        query &= R().type.oneof(parameters['rr_type']['choices'])
    if parameters.get('rr_status') and parameters['rr_status']['all'] is False:
        query &= R().status.oneof(parameters['rr_status']['choices'])
    else:
        query &= R().status.oneof(all_types)
    if parameters.get('mkp') and parameters['mkp']['all'] is False:
        query &= R().asset.marketplace.id.oneof(parameters['mkp']['choices'])
    if parameters.get('hub') and parameters['hub']['all'] is False:
        query &= R().asset.connection.hub.id.oneof(parameters['hub']['choices'])
    # CR20210920
    else:
        query &= R().asset.connection.type.oneof(all_connections)
    # CR20210920

    return client.requests.filter(query)


def _process_line(request, connection):
    return (
        get_basic_value(request, 'id'),
        # CR20210820
        #get_basic_value(request, 'type'),
        #get_basic_value(request, 'status'),
        # CR20210820
        convert_to_datetime(
            get_basic_value(request, 'created'),
        ),
        convert_to_datetime(
            get_basic_value(request, 'updated'),
        ),
        #today_str(),
        get_value(request['asset']['tiers'], 'customer', 'id'),
        get_value(request['asset']['tiers'], 'customer', 'name'),
        # CR20210728 - Adding the taxid
        get_value(request['asset']['tiers'], 'customer', 'tax_id'),
        # CR20210728 - END
        get_value(request['asset']['tiers'], 'customer', 'external_id'),
        # get_value(request['asset']['tiers'], 'tier1', 'id'),
        # get_value(request['asset']['tiers'], 'tier1', 'name'),
        # get_value(request['asset']['tiers'], 'tier1', 'external_id'),
        # get_value(request['asset']['tiers'], 'tier2', 'id'),
        # get_value(request['asset']['tiers'], 'tier2', 'name'),
        # get_value(request['asset']['tiers'], 'tier2', 'external_id'),
        # get_value(request['asset']['connection'], 'provider', 'id'),
        # get_value(request['asset']['connection'], 'provider', 'name'),
        # get_value(request['asset']['connection'], 'vendor', 'id'),
        # get_value(request['asset']['connection'], 'vendor', 'name'),
        # get_value(request['asset'], 'product', 'id'),
        # get_value(request['asset'], 'product', 'name'),
        get_value(request, 'asset', 'id'),
        get_value(request, 'asset', 'external_id'),
        #get_value(request['asset'], 'connection', 'type'),
        #get_value(connection, 'hub', 'id') if 'hub' in connection else '',
        #get_value(connection, 'hub', 'name') if 'hub' in connection else '',
        #get_value(request, 'asset', 'status'),
        # CR20210810 - Adding the specific ordering parameters and reasons
        # CR20210810 - Technical Contact Name
          # CR20210831 - Remove Personal Data
          #get_basic_value(request['asset']['params'][20],'value'),
          # CR20210831 - FIN
        # CR20210810 - Technical Contact Name Error
        # CR20220318 - Change the relative position in asset params from 20 to 23
        # get_basic_value(request['asset']['params'][20],'value_error'),
        # 20220418 - From 23 to 24
        get_basic_value(request['asset']['params'][24],'value_error'),
        # CR20210810 - Technical Contact e-mail
          # CR20210831 - Remove Personal Data
          # get_basic_value(request['asset']['params'][21],'value'),
          # CR20210831 - FIN
        # CR20210810 - Technical Contact e-mail Error
        # CR20220318 - Change the relative position in asset params from 21 to 24
        # get_basic_value(request['asset']['params'][21],'value_error'),
        # 20220418 - From 24 to 25
        get_basic_value(request['asset']['params'][25],'value_error'),
        # CR20210810 - Technical Contact phone
          # CR20210831 - Remove Personal Data
          # get_basic_value(request['asset']['params'][22],'value'),
          # CR20210831 - FIN
        # CR20210810 - Technical Contact phone Error
        # CR20220318 - Change the relative position in asset params from 22 to 25
        # get_basic_value(request['asset']['params'][22],'value_error'),
        # 20220418 - From 25 to 26
        get_basic_value(request['asset']['params'][25],'value_error'),
        # CR20210810 - Domain
        # CR20220318 - Change the relative position in asset params from 14 to 16
        # get_basic_value(request['asset']['params'][14],'value'),
        # 20220418 - From 16 to 17
        get_basic_value(request['asset']['params'][17],'value'),
        # CR20210810 - Domain Error
        # CR20220318 - Change the relative position in asset params from 14 to 16
        # get_basic_value(request['asset']['params'][14],'value_error'),
        # 20220418 - From 16 to 17
        get_basic_value(request['asset']['params'][17],'value_error'),
        # CR20210810 - END
    )
