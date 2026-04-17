#!/usr/bin/env python3
import io
import json
import mimetypes
import threading
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
import uuid
import zipfile
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
HOST = '127.0.0.1'
PORT = 8000
JOBS = {}
JOBS_LOCK = threading.Lock()

TRANSACTION_A_STEPS = [
    ('discover', 'Discover Asset'),
    ('select_offer', 'Select Offer'),
    ('negotiate', 'Initiate Negotiation'),
    ('wait_negotiation', 'Await Agreement'),
    ('transfer', 'Initiate Transfer'),
    ('wait_transfer', 'Await Data Plane'),
    ('get_edr', 'Get EDR'),
    ('download', 'Download Asset 1'),
]

TRANSACTION_B_STEPS = [
    ('discover', 'Discover Asset'),
    ('select_offer', 'Select Offer'),
    ('negotiate', 'Initiate Negotiation'),
    ('wait_negotiation', 'Await Agreement'),
    ('transfer', 'Initiate Transfer'),
    ('wait_transfer', 'Await Data Plane'),
    ('get_edr', 'Get EDR'),
    ('download', 'Download Asset 2'),
]

TRANSACTION_META = {
    'transaction-a': {
        'steps': TRANSACTION_A_STEPS,
        'transaction_name': 'Transaction A',
        'asset_label': 'Asset 1',
        'default_asset_id': 'asset-1-td-real',
        'constraint_default': 'non_critical',
        'download_title': 'Download Asset 1',
        'discover_explanation': 'The backend is asking the federated discovery layer for Asset 1 metadata before any contract is requested.',
        'select_explanation': 'The backend scans the returned policies and picks the offer that matches the manufacturer constraint for Transaction A.',
        'negotiate_explanation': 'The selected offer is now sent to the provider so the governed access agreement can be created.',
        'agreement_explanation': 'This step keeps polling the negotiation resource until the agreement reaches the FINALIZED state.',
        'transfer_explanation': 'With the agreement in place, the backend requests a live transfer for the governed asset.',
        'wait_transfer_explanation': 'The backend polls the transfer resource until the data plane reports that it is ready to serve the governed data.',
        'edr_explanation': 'This step gets the endpoint and authorization material that the consumer will use to pull the protected asset.',
        'download_explanation': 'The backend uses the returned EDR endpoint and authorization token to pull the real asset payload.',
        'success_message': 'Transaction A completed successfully.',
        'success_explanation': 'All eight steps finished successfully. The page now shows the live responses returned by your local environment.',
        'failure_message': 'Transaction A failed before completion.',
        'failure_explanation': 'The run stopped because one of the live backend requests did not complete successfully. Review the error card and the latest response entry for the exact cause.',
        'download_summary_template': '{asset_label} was downloaded from {public_url}.',
        'download_result_explanation': 'This final step proves that the full transaction has completed and the governed vehicle market statistics are now available to the page.',
        'offer_summary_template': 'Selected offer {offer_id} for the manufacturer flow.',
        'agreement_summary_template': 'Negotiation finalized with agreement {agreement_id}.',
        'agreement_result_explanation': 'The provider and consumer now share a valid contract agreement for Asset 1.',
        'edr_summary': 'The EDR data address and authorization token were retrieved successfully.',
        'edr_result_explanation': 'The backend now has the live URL and token needed to fetch Asset 1 from the dataplane.',
    },
    'transaction-b': {
        'steps': TRANSACTION_B_STEPS,
        'transaction_name': 'Transaction B',
        'asset_label': 'Asset 2',
        'default_asset_id': 'epd-real-asset-geojson',
        'constraint_default': 'active',
        'download_title': 'Download Asset 2',
        'discover_explanation': 'The backend is asking the federated discovery layer for Asset 2 metadata before any contract is requested.',
        'select_explanation': 'The backend scans the returned policies and picks the offer that matches the membership constraint for Transaction B.',
        'negotiate_explanation': 'The selected offer is now sent to the provider so the governed access agreement can be created.',
        'agreement_explanation': 'This step keeps polling the negotiation resource until the agreement reaches the FINALIZED state.',
        'transfer_explanation': 'With the agreement in place, the backend requests a live transfer for the governed charging-network asset.',
        'wait_transfer_explanation': 'The backend polls the transfer resource until the data plane reports that it is ready to serve the governed data.',
        'edr_explanation': 'This step gets the endpoint and authorization material that the consumer will use to pull the protected asset.',
        'download_explanation': 'The backend uses the returned EDR endpoint and authorization token to pull the real charging-network payload.',
        'success_message': 'Transaction B completed successfully.',
        'success_explanation': 'All eight steps finished successfully. The page now shows the live responses returned by your local environment.',
        'failure_message': 'Transaction B failed before completion.',
        'failure_explanation': 'The run stopped because one of the live backend requests did not complete successfully. Review the error card and the latest response entry for the exact cause.',
        'download_summary_template': '{asset_label} was downloaded from {public_url}.',
        'download_result_explanation': 'This final step proves that the full transaction has completed and the governed charging-network asset is now available to the page.',
        'offer_summary_template': 'Selected offer {offer_id} for the membership-governed charging flow.',
        'agreement_summary_template': 'Negotiation finalized with agreement {agreement_id}.',
        'agreement_result_explanation': 'The provider and consumer now share a valid contract agreement for Asset 2.',
        'edr_summary': 'The EDR data address and authorization token were retrieved successfully.',
        'edr_result_explanation': 'The backend now has the live URL and token needed to fetch Asset 2 from the dataplane.',
    },
}


def now_ts():
    return time.strftime('%Y-%m-%d %H:%M:%S')


def request_json(url, method='GET', body=None, headers=None, timeout=30):
    headers = headers or {}
    data = None
    if body is not None:
        data = json.dumps(body).encode('utf-8')
        headers.setdefault('Content-Type', 'application/json')
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            content = resp.read()
            text = content.decode('utf-8', errors='replace')
            parsed = None
            if text:
                try:
                    parsed = json.loads(text)
                except json.JSONDecodeError:
                    parsed = text
            return resp.getcode(), dict(resp.headers), parsed, text
    except urllib.error.HTTPError as exc:
        text = exc.read().decode('utf-8', errors='replace')
        parsed = None
        if text:
            try:
                parsed = json.loads(text)
            except json.JSONDecodeError:
                parsed = text
        raise RuntimeError(f'HTTP {exc.code} at {url}: {parsed or text}')
    except urllib.error.URLError as exc:
        raise RuntimeError(f'Unable to reach {url}: {exc.reason}')



def request_binary(url, method='GET', body=None, headers=None, timeout=30):
    headers = headers or {}
    data = None
    if body is not None:
        data = json.dumps(body).encode('utf-8')
        headers.setdefault('Content-Type', 'application/json')

    req = urllib.request.Request(url, data=data, method=method, headers=headers)

    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            content = resp.read()
            return resp.getcode(), dict(resp.headers), content
    except urllib.error.HTTPError as exc:
        raw = exc.read()
        text = raw.decode('utf-8', errors='replace')
        parsed = None
        if text:
            try:
                parsed = json.loads(text)
            except json.JSONDecodeError:
                parsed = text
        raise RuntimeError(f'HTTP {exc.code} at {url}: {parsed or text}')
    except urllib.error.URLError as exc:
        raise RuntimeError(f'Unable to reach {url}: {exc.reason}')


def decode_download_payload(raw_content, response_headers):
    content_type = (response_headers.get('Content-Type') or '').lower()

    if raw_content[:2] == b'PK':
        with zipfile.ZipFile(io.BytesIO(raw_content)) as zf:
            geojson_name = next(
                (name for name in zf.namelist() if name.lower().endswith('.geojson')),
                None
            )
            if not geojson_name:
                raise RuntimeError('The downloaded zip does not contain a .geojson file.')

            geojson_bytes = zf.read(geojson_name)
            geojson_text = geojson_bytes.decode('utf-8')
            return json.loads(geojson_text)

    text = raw_content.decode('utf-8', errors='replace')

    if (
        'application/json' in content_type
        or 'application/geo+json' in content_type
        or text.lstrip().startswith('{')
        or text.lstrip().startswith('[')
    ):
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return text

    return text


def base_headers(config):
    headers = {}
    api_key = config.get('x_api_key', '').strip()
    if api_key:
        headers['x-api-key'] = api_key
    return headers


def init_job(job_id, meta):
    return {
        'job_id': job_id,
        'status': 'running',
        'current_message': f"Starting {meta['transaction_name']}…",
        'current_explanation': 'The backend is preparing the live dataspace workflow.',
        'error': '',
        'steps': [{'key': key, 'title': title, 'status': 'pending'} for key, title in meta['steps']],
        'logs': [],
        'created_at': now_ts(),
    }


def mutate_job(job_id, func):
    with JOBS_LOCK:
        job = JOBS[job_id]
        func(job)


def set_step_status(job_id, key, status):
    def _update(job):
        for step in job['steps']:
            if step['key'] == key:
                step['status'] = status
    mutate_job(job_id, _update)


def set_job_state(job_id, *, status=None, message=None, explanation=None, error=None):
    def _update(job):
        if status is not None:
            job['status'] = status
        if message is not None:
            job['current_message'] = message
        if explanation is not None:
            job['current_explanation'] = explanation
        if error is not None:
            job['error'] = error
    mutate_job(job_id, _update)


def set_job_fields(job_id, **fields):
    def _update(job):
        job.update(fields)
    mutate_job(job_id, _update)


def add_log(job_id, *, step, title, status, summary, explanation, request_method=None, request_url=None, request_body=None, response_status=None, response_body=None):
    def _update(job):
        job['logs'].append({
            'timestamp': now_ts(),
            'step': step,
            'title': title,
            'status': status,
            'summary': summary,
            'explanation': explanation,
            'request_method': request_method,
            'request_url': request_url,
            'request_body': request_body,
            'response_status': response_status,
            'response_body': response_body,
        })
    mutate_job(job_id, _update)


def find_dataset(catalog, asset_id):
    if isinstance(catalog, list):
        for item in catalog:
            result = find_dataset(item, asset_id)
            if result:
                return result
        return None
    if not isinstance(catalog, dict):
        return None
    datasets = catalog.get('dataset') or catalog.get('dcat:dataset') or []
    for dataset in datasets:
        if dataset.get('@id') == asset_id or dataset.get('id') == asset_id:
            return dataset
    return None


def choose_offer(dataset, constraint_right):
    offers = dataset.get('hasPolicy') or []
    if not isinstance(offers, list):
        offers = [offers]

    def constraints_match(offer):
        for area in ('obligation', 'permission'):
            for entry in offer.get(area, []) or []:
                constraints = entry.get('constraint', []) or []
                if isinstance(constraints, dict):
                    constraints = [constraints]
                for constraint in constraints:
                    right = str(constraint.get('rightOperand', ''))
                    if right == constraint_right:
                        return True
        return False

    for offer in offers:
        if constraints_match(offer):
            return offer
    for offer in offers:
        oid = str(offer.get('@id', ''))
        if constraint_right in oid:
            return offer
    return offers[0] if offers else None


def poll_until(job_id, url, matcher, *, method='GET', body=None, headers=None, timeout_seconds=40, interval=2, title='Poll', step='poll'):
    start = time.time()
    last_payload = None
    while time.time() - start < timeout_seconds:
        status_code, _, payload, _ = request_json(url, method=method, body=body, headers=headers, timeout=30)
        last_payload = payload
        add_log(
            job_id,
            step=step,
            title=title,
            status='running',
            summary='Polling the live environment for the next state transition.',
            explanation='The backend keeps checking the current status until the expected lifecycle state appears.',
            request_method=method,
            request_url=url,
            request_body=body,
            response_status=str(status_code),
            response_body=payload,
        )
        if matcher(payload):
            return payload
        time.sleep(interval)
    raise RuntimeError(f'Timeout while waiting for {title}. Last payload: {last_payload}')


def extract_constraint_value(config, meta):
    for key in ('constraint_right', 'manufacturer_constraint_right', 'membership_constraint_right'):
        value = str(config.get(key, '')).strip()
        if value:
            return value
    return meta['constraint_default']


def run_transaction(job_id, config, meta):
    try:
        headers = base_headers(config)
        asset_id = str(config.get('asset_id', meta['default_asset_id'])).strip() or meta['default_asset_id']
        protocol = str(config.get('protocol', 'dataspace-protocol-http:2025-1')).strip() or 'dataspace-protocol-http:2025-1'
        counter_party_address = str(config.get('counter_party_address', '')).strip()
        counter_party_id = str(config.get('counter_party_id', '')).strip()
        constraint_right = extract_constraint_value(config, meta)

        # 1 Discover
        set_step_status(job_id, 'discover', 'running')
        set_job_state(job_id, message='Running catalog discovery…', explanation=meta['discover_explanation'])
        catalog_body = {
            '@context': ['https://w3id.org/edc/connector/management/v0.0.1'],
            '@type': 'CatalogRequest',
            'counterPartyAddress': counter_party_address,
            'counterPartyId': counter_party_id,
            'protocol': protocol,
            'querySpec': {'@type': 'QuerySpec', 'offset': 0, 'limit': 50},
        }
        status_code, _, catalog_payload, _ = request_json(config['catalog_url'], method='POST', body=catalog_body, headers=headers)
        dataset = find_dataset(catalog_payload, asset_id)
        if not dataset:
            raise RuntimeError(f'Asset {asset_id} was not found in the catalog response.')
        add_log(
            job_id, step='discover', title='Discover Asset', status='completed',
            summary=f'The catalog returned metadata for {asset_id}.', explanation='This confirms that the asset is discoverable before any usage agreement is created.',
            request_method='POST', request_url=config['catalog_url'], request_body=catalog_body, response_status=str(status_code), response_body=catalog_payload,
        )
        set_step_status(job_id, 'discover', 'completed')

        # 2 Select offer
        set_step_status(job_id, 'select_offer', 'running')
        set_job_state(job_id, message='Selecting the compatible offer…', explanation=meta['select_explanation'])
        offer = choose_offer(dataset, constraint_right)
        if not offer:
            raise RuntimeError('No compatible offer was found for the discovered asset.')
        offer = dict(offer)
        offer.setdefault('target', asset_id)
        offer.setdefault('assigner', counter_party_id)
        add_log(
            job_id, step='select_offer', title='Select Offer', status='completed',
            summary=meta['offer_summary_template'].format(offer_id=offer.get('@id', 'unknown-offer')), explanation='The chosen offer is the policy object that will be sent into contract negotiation.',
            request_method='POST', request_url=config['catalog_url'], request_body={'asset_id': asset_id, 'constraint_right': constraint_right}, response_status='derived', response_body=offer,
        )
        set_step_status(job_id, 'select_offer', 'completed')

        # 3 Negotiate
        set_step_status(job_id, 'negotiate', 'running')
        set_job_state(job_id, message='Submitting the contract negotiation request…', explanation=meta['negotiate_explanation'])
        negotiation_body = {
            '@context': ['https://w3id.org/edc/connector/management/v0.0.1'],
            '@type': 'ContractRequest',
            'counterPartyAddress': counter_party_address,
            'counterPartyId': counter_party_id,
            'protocol': protocol,
            'policy': offer,
            'callbackAddresses': [],
        }
        status_code, _, negotiation_payload, _ = request_json(config['negotiation_url'], method='POST', body=negotiation_body, headers=headers)
        negotiation_id = negotiation_payload.get('@id') if isinstance(negotiation_payload, dict) else None
        if not negotiation_id:
            raise RuntimeError(f'Negotiation id was not returned. Payload: {negotiation_payload}')
        add_log(
            job_id, step='negotiate', title='Initiate Negotiation', status='completed',
            summary=f'Negotiation {negotiation_id} was created successfully.', explanation='The provider has accepted the request format and created a live contract negotiation resource.',
            request_method='POST', request_url=config['negotiation_url'], request_body=negotiation_body, response_status=str(status_code), response_body=negotiation_payload,
        )
        set_step_status(job_id, 'negotiate', 'completed')

        # 4 Await Agreement
        set_step_status(job_id, 'wait_negotiation', 'running')
        set_job_state(job_id, message='Waiting for the negotiation to finalize…', explanation=meta['agreement_explanation'])
        negotiation_state_url = f"{config['negotiation_url'].rstrip('/')}/{urllib.parse.quote(negotiation_id)}"
        def negotiation_done(payload):
            return isinstance(payload, dict) and payload.get('state') == 'FINALIZED'
        negotiation_final = poll_until(job_id, negotiation_state_url, negotiation_done, headers=headers, title='Await Agreement', step='wait_negotiation')
        agreement_id = negotiation_final.get('contractAgreementId')
        if not agreement_id:
            raise RuntimeError(f'No contractAgreementId was returned after finalization. Payload: {negotiation_final}')
        add_log(
            job_id, step='wait_negotiation', title='Await Agreement', status='completed',
            summary=meta['agreement_summary_template'].format(agreement_id=agreement_id), explanation=meta['agreement_result_explanation'],
            request_method='GET', request_url=negotiation_state_url, request_body=None, response_status='FINALIZED', response_body=negotiation_final,
        )
        set_step_status(job_id, 'wait_negotiation', 'completed')

        # 5 Transfer
        set_step_status(job_id, 'transfer', 'running')
        set_job_state(job_id, message='Starting the transfer process…', explanation=meta['transfer_explanation'])
        transfer_body = {
            '@context': ['https://w3id.org/edc/connector/management/v0.0.1'],
            '@type': 'TransferRequest',
            'counterPartyAddress': counter_party_address,
            'counterPartyId': counter_party_id,
            'contractId': agreement_id,
            'assetId': asset_id,
            'protocol': protocol,
            'transferType': 'HttpData-PULL',
            'dataDestination': {'type': 'HttpProxy'},
            'managedResources': False,
            'callbackAddresses': [],
        }
        status_code, _, transfer_payload, _ = request_json(config['transfer_url'], method='POST', body=transfer_body, headers=headers)
        transfer_id = transfer_payload.get('@id') if isinstance(transfer_payload, dict) else None
        if not transfer_id:
            raise RuntimeError(f'Transfer process id was not returned. Payload: {transfer_payload}')
        add_log(
            job_id, step='transfer', title='Initiate Transfer', status='completed',
            summary=f'Transfer process {transfer_id} was created.', explanation='The data plane handshake has been triggered and the transfer resource now exists.',
            request_method='POST', request_url=config['transfer_url'], request_body=transfer_body, response_status=str(status_code), response_body=transfer_payload,
        )
        set_step_status(job_id, 'transfer', 'completed')

        # 6 Await Data Plane
        set_step_status(job_id, 'wait_transfer', 'running')
        set_job_state(job_id, message='Waiting for the transfer process to start…', explanation=meta['wait_transfer_explanation'])
        transfer_state_url = f"{config['transfer_url'].rstrip('/')}/{urllib.parse.quote(transfer_id)}"
        def transfer_started(payload):
            if isinstance(payload, dict):
                return payload.get('state') == 'STARTED'
            if isinstance(payload, list) and payload:
                return payload[0].get('state') == 'STARTED'
            return False
        try:
            transfer_final = poll_until(job_id, transfer_state_url, transfer_started, headers=headers, title='Await Data Plane', step='wait_transfer')
        except Exception:
            query_body = {
                '@context': ['https://w3id.org/edc/connector/management/v0.0.1'],
                '@type': 'QuerySpec',
                'filterExpression': [{'operandLeft': 'id', 'operator': '=', 'operandRight': transfer_id}],
            }
            transfer_final = poll_until(job_id, config['transfer_query_url'], transfer_started, method='POST', body=query_body, headers=headers, title='Await Data Plane', step='wait_transfer')
            if isinstance(transfer_final, list):
                transfer_final = transfer_final[0]
        add_log(
            job_id, step='wait_transfer', title='Await Data Plane', status='completed',
            summary=f'Transfer process {transfer_id} reached STARTED.', explanation='The transfer has moved into the live state required before requesting the EDR token.',
            request_method='GET/POST', request_url=transfer_state_url, request_body=None, response_status='STARTED', response_body=transfer_final,
        )
        set_step_status(job_id, 'wait_transfer', 'completed')

        # 7 Get EDR
        set_step_status(job_id, 'get_edr', 'running')
        set_job_state(job_id, message='Retrieving the EDR data address…', explanation=meta['edr_explanation'])
        edr_url = f"{config['edr_base_url'].rstrip('/')}/{urllib.parse.quote(transfer_id)}/dataaddress"
        status_code, _, edr_payload, _ = request_json(edr_url, method='GET', headers=headers)
        if not isinstance(edr_payload, dict):
            raise RuntimeError(f'Unexpected EDR response: {edr_payload}')
        endpoint = edr_payload.get('endpoint') or edr_payload.get('endpointUrl') or edr_payload.get('edc:endpoint') or str(config.get('final_public_url', '')).strip()
        authorization = edr_payload.get('authorization') or edr_payload.get('authCode') or edr_payload.get('edc:authorization')
        print('DEBUG authorization raw =', repr(authorization), flush=True)
        print('DEBUG edr payload =', repr(edr_payload), flush=True)
        if not endpoint:
            raise RuntimeError(f'No endpoint was returned in the EDR payload: {edr_payload}')
        add_log(
            job_id, step='get_edr', title='Get EDR', status='completed',
            summary=meta['edr_summary'], explanation=meta['edr_result_explanation'],
            request_method='GET', request_url=edr_url, request_body=None, response_status=str(status_code), response_body=edr_payload,
        )
        set_step_status(job_id, 'get_edr', 'completed')

        # 8 Download
        set_step_status(job_id, 'download', 'running')
        set_job_state(job_id, message=f"Downloading the {meta['asset_label'].lower()} payload…", explanation=meta['download_explanation'])
        download_headers = {}
        if authorization:
            auth_value = str(authorization).strip()
            if auth_value.lower().startswith('bearer '):
                auth_value = auth_value[7:].strip()
            download_headers['Authorization'] = auth_value
        public_url = str(config.get('final_public_url', '')).strip() or endpoint
        status_code, response_headers, raw_content = request_binary(public_url, method='GET', headers=download_headers, timeout=60)
        response_body = decode_download_payload(raw_content, response_headers)
        add_log(
            job_id, step='download', title=meta['download_title'], status='completed',
            summary=meta['download_summary_template'].format(asset_label=meta['asset_label'], public_url=public_url), explanation=meta['download_result_explanation'],
            request_method='GET', request_url=public_url, request_body=None, response_status=str(status_code), response_body=response_body,
        )
        set_step_status(job_id, 'download', 'completed')
        set_job_state(job_id, status='completed', message=meta['success_message'], explanation=meta['success_explanation'])
    except Exception as exc:
        error_message = f'{exc}\n\n{traceback.format_exc(limit=3)}'
        set_job_state(job_id, status='failed', message=meta['failure_message'], explanation=meta['failure_explanation'], error=error_message)
        add_log(
            job_id, step='error', title='Workflow Error', status='failed',
            summary='The live transaction stopped because a backend request failed.', explanation='This error was returned by the local orchestration layer while it was talking to your running dataspace services.',
            response_body=error_message,
        )


CLIENT_TRANSACTION_META = {
    'client-transaction-a': {
        'transaction_name': 'Client Transaction A',
        'asset_label': 'Vehicle Market Statistics',
        'default_asset_id': 'asset-1-td-real',
        'constraint_default': 'non_critical',
        'success_message': 'Authorized result is ready.',
        'failure_message': 'The request could not be completed.',
    },
    'client-transaction-b': {
        'transaction_name': 'Client Transaction B',
        'asset_label': 'EV-Charging Network',
        'default_asset_id': 'epd-real-asset-geojson',
        'constraint_default': 'active',
        'success_message': 'Authorized result is ready.',
        'failure_message': 'The request could not be completed.',
    },
}


def safe_error_message(exc):
    return str(exc)


def client_catalog_request(config, headers, counter_party_address, counter_party_id, protocol):
    catalog_body = {
        '@context': ['https://w3id.org/edc/connector/management/v0.0.1'],
        '@type': 'CatalogRequest',
        'counterPartyAddress': counter_party_address,
        'counterPartyId': counter_party_id,
        'protocol': protocol,
        'querySpec': {'@type': 'QuerySpec', 'offset': 0, 'limit': 50},
    }
    status_code, _, catalog_payload, _ = request_json(config['catalog_url'], method='POST', body=catalog_body, headers=headers)
    return status_code, catalog_body, catalog_payload


def run_client_transaction_a(job_id, config):
    meta = CLIENT_TRANSACTION_META['client-transaction-a']
    try:
        headers = base_headers(config)
        asset_id = str(config.get('asset_id', meta['default_asset_id'])).strip() or meta['default_asset_id']
        protocol = str(config.get('protocol', 'dataspace-protocol-http:2025-1')).strip() or 'dataspace-protocol-http:2025-1'
        counter_party_address = str(config.get('counter_party_address', '')).strip()
        counter_party_id = str(config.get('counter_party_id', '')).strip()
        constraint_right = str(config.get('manufacturer_constraint_right', meta['constraint_default'])).strip() or meta['constraint_default']

        set_job_state(job_id, message='Checking participant membership...', explanation='The backend is verifying that this participant can discover the governed asset under the membership policy.')
        set_step_status(job_id, 'discover', 'running')
        status_code, catalog_body, catalog_payload = client_catalog_request(config, headers, counter_party_address, counter_party_id, protocol)
        dataset = find_dataset(catalog_payload, asset_id)
        if not dataset:
            raise RuntimeError('This service is available only to verified dataspace members.')
        add_log(job_id, step='discover', title='Membership Check', status='completed', summary='Membership verification passed through governed discovery.', explanation='The asset is visible to this participant under the discovery policy.', request_method='POST', request_url=config['catalog_url'], request_body=catalog_body, response_status=str(status_code), response_body=catalog_payload)
        set_step_status(job_id, 'discover', 'completed')
        set_job_fields(job_id, authorization_checks={'membership': True})

        set_job_state(job_id, message='Checking manufacturer authorization...', explanation='The backend is verifying that the manufacturer role matches the governed contract policy.')
        set_step_status(job_id, 'select_offer', 'running')
        offer = choose_offer(dataset, constraint_right)
        if not offer:
            raise RuntimeError('This service is restricted to participants with the required manufacturer role.')
        offer = dict(offer)
        offer.setdefault('target', asset_id)
        offer.setdefault('assigner', counter_party_id)
        negotiation_body = {
            '@context': ['https://w3id.org/edc/connector/management/v0.0.1'],
            '@type': 'ContractRequest',
            'counterPartyAddress': counter_party_address,
            'counterPartyId': counter_party_id,
            'protocol': protocol,
            'policy': offer,
            'callbackAddresses': [],
        }
        status_code, _, negotiation_payload, _ = request_json(config['negotiation_url'], method='POST', body=negotiation_body, headers=headers)
        negotiation_id = negotiation_payload.get('@id') if isinstance(negotiation_payload, dict) else None
        if not negotiation_id:
            raise RuntimeError('This service is restricted to participants with the required manufacturer role.')
        add_log(job_id, step='select_offer', title='Manufacturer Authorization', status='completed', summary='Manufacturer authorization passed and a governed agreement request was created.', explanation='The provider accepted the role-governed contract request for this participant.', request_method='POST', request_url=config['negotiation_url'], request_body=negotiation_body, response_status=str(status_code), response_body=negotiation_payload)
        set_step_status(job_id, 'select_offer', 'completed')
        set_job_fields(job_id, authorization_checks={'membership': True, 'manufacturer_role': True})

        set_job_state(job_id, message='Authorization confirmed. Preparing access request...', explanation='A valid governed agreement is now being finalized before data transfer starts.')
        set_step_status(job_id, 'wait_negotiation', 'running')
        negotiation_state_url = f"{config['negotiation_url'].rstrip('/')}/{urllib.parse.quote(negotiation_id)}"
        def negotiation_done(payload):
            return isinstance(payload, dict) and payload.get('state') == 'FINALIZED'
        negotiation_final = poll_until(job_id, negotiation_state_url, negotiation_done, headers=headers, title='Await Agreement', step='wait_negotiation')
        agreement_id = negotiation_final.get('contractAgreementId')
        if not agreement_id:
            raise RuntimeError('A contract agreement could not be finalized for this request.')
        add_log(job_id, step='wait_negotiation', title='Agreement Ready', status='completed', summary=f'Agreement {agreement_id} finalized successfully.', explanation='The governed agreement is active and transfer can begin.', request_method='GET', request_url=negotiation_state_url, request_body=None, response_status='FINALIZED', response_body=negotiation_final)
        set_step_status(job_id, 'wait_negotiation', 'completed')

        set_step_status(job_id, 'transfer', 'running')
        set_job_state(job_id, message='Preparing authorized result transfer...', explanation='The backend is requesting the governed asset through the transfer process.')
        transfer_body = {
            '@context': ['https://w3id.org/edc/connector/management/v0.0.1'],
            '@type': 'TransferRequest',
            'counterPartyAddress': counter_party_address,
            'counterPartyId': counter_party_id,
            'contractId': agreement_id,
            'assetId': asset_id,
            'protocol': protocol,
            'transferType': 'HttpData-PULL',
            'dataDestination': {'type': 'HttpProxy'},
            'managedResources': False,
            'callbackAddresses': [],
        }
        status_code, _, transfer_payload, _ = request_json(config['transfer_url'], method='POST', body=transfer_body, headers=headers)
        transfer_id = transfer_payload.get('@id') if isinstance(transfer_payload, dict) else None
        if not transfer_id:
            raise RuntimeError('The transfer process could not be created.')
        add_log(job_id, step='transfer', title='Transfer Request', status='completed', summary=f'Transfer process {transfer_id} was created.', explanation='The authorized data transfer request has been accepted.', request_method='POST', request_url=config['transfer_url'], request_body=transfer_body, response_status=str(status_code), response_body=transfer_payload)
        set_step_status(job_id, 'transfer', 'completed')

        set_step_status(job_id, 'wait_transfer', 'running')
        set_job_state(job_id, message='Preparing authorized result...', explanation='The backend is waiting for the data plane to make the authorized asset available.')
        transfer_state_url = f"{config['transfer_url'].rstrip('/')}/{urllib.parse.quote(transfer_id)}"
        def transfer_started(payload):
            if isinstance(payload, dict):
                return payload.get('state') == 'STARTED'
            if isinstance(payload, list) and payload:
                return payload[0].get('state') == 'STARTED'
            return False
        try:
            transfer_final = poll_until(job_id, transfer_state_url, transfer_started, headers=headers, title='Await Data Plane', step='wait_transfer')
        except Exception:
            query_body = {
                '@context': ['https://w3id.org/edc/connector/management/v0.0.1'],
                '@type': 'QuerySpec',
                'filterExpression': [{'operandLeft': 'id', 'operator': '=', 'operandRight': transfer_id}],
            }
            transfer_final = poll_until(job_id, config['transfer_query_url'], transfer_started, method='POST', body=query_body, headers=headers, title='Await Data Plane', step='wait_transfer')
            if isinstance(transfer_final, list):
                transfer_final = transfer_final[0]
        add_log(job_id, step='wait_transfer', title='Transfer Ready', status='completed', summary='The authorized transfer reached the STARTED state.', explanation='The asset is now ready to be pulled from the data plane.', request_method='GET/POST', request_url=transfer_state_url, request_body=None, response_status='STARTED', response_body=transfer_final)
        set_step_status(job_id, 'wait_transfer', 'completed')

        set_step_status(job_id, 'get_edr', 'running')
        set_job_state(job_id, message='Opening authorized result channel...', explanation='The backend is retrieving the live data address and authorization token.')
        edr_url = f"{config['edr_base_url'].rstrip('/')}/{urllib.parse.quote(transfer_id)}/dataaddress"
        status_code, _, edr_payload, _ = request_json(edr_url, method='GET', headers=headers)
        if not isinstance(edr_payload, dict):
            raise RuntimeError('The live data address could not be retrieved.')
        endpoint = edr_payload.get('endpoint') or edr_payload.get('endpointUrl') or edr_payload.get('edc:endpoint') or str(config.get('final_public_url', '')).strip()
        authorization = edr_payload.get('authorization') or edr_payload.get('authCode') or edr_payload.get('edc:authorization')
        print('DEBUG authorization raw =', repr(authorization), flush=True)
        print('DEBUG edr payload =', repr(edr_payload), flush=True)
        if not endpoint:
            raise RuntimeError('The live data address could not be retrieved.')
        add_log(job_id, step='get_edr', title='Open Result Channel', status='completed', summary='The live endpoint and authorization material were retrieved.', explanation='The consumer now has a governed channel for reading the authorized result.', request_method='GET', request_url=edr_url, request_body=None, response_status=str(status_code), response_body=edr_payload)
        set_step_status(job_id, 'get_edr', 'completed')

        set_step_status(job_id, 'download', 'running')
        set_job_state(job_id, message='Preparing controlled result view...', explanation='The backend is pulling the governed payload that will feed the controlled preview.')
        download_headers = {}
        if authorization:
            auth_value = str(authorization).strip()
            if auth_value.lower().startswith('bearer '):
                auth_value = auth_value[7:].strip()
            download_headers['Authorization'] = auth_value
        public_url = str(config.get('final_public_url', '')).strip() or endpoint
        status_code, response_headers, raw_content = request_binary(public_url, method='GET', headers=download_headers, timeout=60)
        response_body = decode_download_payload(raw_content, response_headers)
        add_log(job_id, step='download', title='Prepare Result', status='completed', summary='The controlled vehicle-market result is ready for preview.', explanation='The page can now render an authorized preview and export a permitted copy.', request_method='GET', request_url=public_url, request_body=None, response_status=str(status_code), response_body=response_body)
        set_step_status(job_id, 'download', 'completed')
        set_job_state(job_id, status='completed', message=meta['success_message'], explanation='The authorized market view can now be opened.')
    except Exception as exc:
        set_job_state(job_id, status='failed', message=meta['failure_message'], explanation='The governed request stopped before a controlled result could be prepared.', error=safe_error_message(exc))
        add_log(job_id, step='error', title='Workflow Error', status='failed', summary='The request failed before the authorized result was ready.', explanation='The local orchestration layer received an error from the running dataspace environment.', response_body=safe_error_message(exc))


def run_client_transaction_b(job_id, config):
    meta = CLIENT_TRANSACTION_META['client-transaction-b']
    try:
        headers = base_headers(config)
        asset_id = str(config.get('asset_id', meta['default_asset_id'])).strip() or meta['default_asset_id']
        protocol = str(config.get('protocol', 'dataspace-protocol-http:2025-1')).strip() or 'dataspace-protocol-http:2025-1'
        counter_party_address = str(config.get('counter_party_address', '')).strip()
        counter_party_id = str(config.get('counter_party_id', '')).strip()
        constraint_right = str(config.get('constraint_right', meta['constraint_default'])).strip() or meta['constraint_default']

        set_job_state(job_id, message='Checking participant membership...', explanation='The backend is verifying that this participant can discover the governed charging asset under the membership policy.')
        set_step_status(job_id, 'discover', 'running')
        status_code, catalog_body, catalog_payload = client_catalog_request(config, headers, counter_party_address, counter_party_id, protocol)
        dataset = find_dataset(catalog_payload, asset_id)
        if not dataset:
            raise RuntimeError('This service is available only to verified dataspace members.')
        add_log(job_id, step='discover', title='Membership Check', status='completed', summary='Membership verification passed through governed discovery.', explanation='The charging asset is visible to this participant under the membership policy.', request_method='POST', request_url=config['catalog_url'], request_body=catalog_body, response_status=str(status_code), response_body=catalog_payload)
        set_step_status(job_id, 'discover', 'completed')
        set_job_fields(job_id, authorization_checks={'membership': True})

        offer = choose_offer(dataset, constraint_right)
        if not offer:
            raise RuntimeError('No compatible member-governed offer was available for this service.')
        offer = dict(offer)
        offer.setdefault('target', asset_id)
        offer.setdefault('assigner', counter_party_id)

        set_job_state(job_id, message='Authorization confirmed. Preparing access request...', explanation='A membership-governed contract request is being created for the charging asset.')
        set_step_status(job_id, 'select_offer', 'running')
        negotiation_body = {
            '@context': ['https://w3id.org/edc/connector/management/v0.0.1'],
            '@type': 'ContractRequest',
            'counterPartyAddress': counter_party_address,
            'counterPartyId': counter_party_id,
            'protocol': protocol,
            'policy': offer,
            'callbackAddresses': [],
        }
        status_code, _, negotiation_payload, _ = request_json(config['negotiation_url'], method='POST', body=negotiation_body, headers=headers)
        negotiation_id = negotiation_payload.get('@id') if isinstance(negotiation_payload, dict) else None
        if not negotiation_id:
            raise RuntimeError('A governed access request could not be created for this member flow.')
        add_log(job_id, step='select_offer', title='Prepare Access Request', status='completed', summary='The member-governed contract request was created successfully.', explanation='The provider accepted the membership-governed request for this asset.', request_method='POST', request_url=config['negotiation_url'], request_body=negotiation_body, response_status=str(status_code), response_body=negotiation_payload)
        set_step_status(job_id, 'select_offer', 'completed')

        set_step_status(job_id, 'wait_negotiation', 'running')
        set_job_state(job_id, message='Finalizing governed agreement...', explanation='The backend is waiting for the governed agreement to become active.')
        negotiation_state_url = f"{config['negotiation_url'].rstrip('/')}/{urllib.parse.quote(negotiation_id)}"
        def negotiation_done(payload):
            return isinstance(payload, dict) and payload.get('state') == 'FINALIZED'
        negotiation_final = poll_until(job_id, negotiation_state_url, negotiation_done, headers=headers, title='Await Agreement', step='wait_negotiation')
        agreement_id = negotiation_final.get('contractAgreementId')
        if not agreement_id:
            raise RuntimeError('A contract agreement could not be finalized for this charging request.')
        add_log(job_id, step='wait_negotiation', title='Agreement Ready', status='completed', summary=f'Agreement {agreement_id} finalized successfully.', explanation='The governed agreement is active and charging-data transfer can begin.', request_method='GET', request_url=negotiation_state_url, request_body=None, response_status='FINALIZED', response_body=negotiation_final)
        set_step_status(job_id, 'wait_negotiation', 'completed')

        set_step_status(job_id, 'transfer', 'running')
        set_job_state(job_id, message='Preparing authorized result transfer...', explanation='The backend is requesting the governed charging asset through the transfer process.')
        transfer_body = {
            '@context': ['https://w3id.org/edc/connector/management/v0.0.1'],
            '@type': 'TransferRequest',
            'counterPartyAddress': counter_party_address,
            'counterPartyId': counter_party_id,
            'contractId': agreement_id,
            'assetId': asset_id,
            'protocol': protocol,
            'transferType': 'HttpData-PULL',
            'dataDestination': {'type': 'HttpProxy'},
            'managedResources': False,
            'callbackAddresses': [],
        }
        status_code, _, transfer_payload, _ = request_json(config['transfer_url'], method='POST', body=transfer_body, headers=headers)
        transfer_id = transfer_payload.get('@id') if isinstance(transfer_payload, dict) else None
        if not transfer_id:
            raise RuntimeError('The transfer process could not be created.')
        add_log(job_id, step='transfer', title='Transfer Request', status='completed', summary=f'Transfer process {transfer_id} was created.', explanation='The authorized charging-data transfer request has been accepted.', request_method='POST', request_url=config['transfer_url'], request_body=transfer_body, response_status=str(status_code), response_body=transfer_payload)
        set_step_status(job_id, 'transfer', 'completed')

        set_step_status(job_id, 'wait_transfer', 'running')
        set_job_state(job_id, message='Preparing authorized result...', explanation='The backend is waiting for the data plane to make the authorized charging result available.')
        transfer_state_url = f"{config['transfer_url'].rstrip('/')}/{urllib.parse.quote(transfer_id)}"
        def transfer_started(payload):
            if isinstance(payload, dict):
                return payload.get('state') == 'STARTED'
            if isinstance(payload, list) and payload:
                return payload[0].get('state') == 'STARTED'
            return False
        try:
            transfer_final = poll_until(job_id, transfer_state_url, transfer_started, headers=headers, title='Await Data Plane', step='wait_transfer')
        except Exception:
            query_body = {
                '@context': ['https://w3id.org/edc/connector/management/v0.0.1'],
                '@type': 'QuerySpec',
                'filterExpression': [{'operandLeft': 'id', 'operator': '=', 'operandRight': transfer_id}],
            }
            transfer_final = poll_until(job_id, config['transfer_query_url'], transfer_started, method='POST', body=query_body, headers=headers, title='Await Data Plane', step='wait_transfer')
            if isinstance(transfer_final, list):
                transfer_final = transfer_final[0]
        add_log(job_id, step='wait_transfer', title='Transfer Ready', status='completed', summary='The authorized transfer reached the STARTED state.', explanation='The charging result is now ready to be pulled from the data plane.', request_method='GET/POST', request_url=transfer_state_url, request_body=None, response_status='STARTED', response_body=transfer_final)
        set_step_status(job_id, 'wait_transfer', 'completed')

        set_step_status(job_id, 'get_edr', 'running')
        set_job_state(job_id, message='Opening authorized result channel...', explanation='The backend is retrieving the live data address and authorization token.')
        edr_url = f"{config['edr_base_url'].rstrip('/')}/{urllib.parse.quote(transfer_id)}/dataaddress"
        status_code, _, edr_payload, _ = request_json(edr_url, method='GET', headers=headers)
        if not isinstance(edr_payload, dict):
            raise RuntimeError('The live data address could not be retrieved.')
        endpoint = edr_payload.get('endpoint') or edr_payload.get('endpointUrl') or edr_payload.get('edc:endpoint') or str(config.get('final_public_url', '')).strip()
        authorization = edr_payload.get('authorization') or edr_payload.get('authCode') or edr_payload.get('edc:authorization')
        print('DEBUG authorization raw =', repr(authorization), flush=True)
        print('DEBUG edr payload =', repr(edr_payload), flush=True)
        if not endpoint:
            raise RuntimeError('The live data address could not be retrieved.')
        add_log(job_id, step='get_edr', title='Open Result Channel', status='completed', summary='The live endpoint and authorization material were retrieved.', explanation='The consumer now has a governed channel for reading the authorized charging result.', request_method='GET', request_url=edr_url, request_body=None, response_status=str(status_code), response_body=edr_payload)
        set_step_status(job_id, 'get_edr', 'completed')

        set_step_status(job_id, 'download', 'running')
        set_job_state(job_id, message='Preparing controlled result view...', explanation='The backend is pulling the governed charging payload that will feed the controlled preview.')
        download_headers = {}
        if authorization:
            auth_value = str(authorization).strip()
            if auth_value.lower().startswith('bearer '):
                auth_value = auth_value[7:].strip()
            download_headers['Authorization'] = auth_value
        public_url = str(config.get('final_public_url', '')).strip() or endpoint
        status_code, response_headers, raw_content = request_binary(public_url, method='GET', headers=download_headers, timeout=60)
        response_body = decode_download_payload(raw_content, response_headers)
        add_log(job_id, step='download', title='Prepare Result', status='completed', summary='The controlled charging result is ready for preview.', explanation='The page can now render an authorized view of the charging network data.', request_method='GET', request_url=public_url, request_body=None, response_status=str(status_code), response_body=response_body)
        set_step_status(job_id, 'download', 'completed')
        set_job_state(job_id, status='completed', message=meta['success_message'], explanation='The authorized charging view can now be opened.')
    except Exception as exc:
        set_job_state(job_id, status='failed', message=meta['failure_message'], explanation='The governed request stopped before a controlled result could be prepared.', error=safe_error_message(exc))
        add_log(job_id, step='error', title='Workflow Error', status='failed', summary='The request failed before the authorized result was ready.', explanation='The local orchestration layer received an error from the running dataspace environment.', response_body=safe_error_message(exc))



class AppHandler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'

    def log_message(self, fmt, *args):
        return

    def _set_cors_headers(self):
        origin = self.headers.get('Origin')
        if origin:
            self.send_header('Access-Control-Allow-Origin', origin)
            self.send_header('Vary', 'Origin')
        else:
            self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type, x-api-key')
        self.send_header('Access-Control-Max-Age', '86400')

    def _send_bytes(self, code, data, content_type='application/octet-stream'):
        self.send_response(code)
        self.send_header('Content-Type', content_type)
        self.send_header('Content-Length', str(len(data)))
        self.send_header('Cache-Control', 'no-store')
        self._set_cors_headers()
        self.end_headers()
        self.wfile.write(data)

    def _send_json(self, code, payload):
        self._send_bytes(code, json.dumps(payload, ensure_ascii=False).encode('utf-8'), 'application/json; charset=utf-8')

    def _read_json_body(self):
        length = int(self.headers.get('Content-Length', '0'))
        raw = self.rfile.read(length) if length else b'{}'
        if not raw:
            return {}
        return json.loads(raw.decode('utf-8'))

    def do_OPTIONS(self):
        self.send_response(204)
        self._set_cors_headers()
        self.end_headers()

    def do_GET(self):
        if self.path == '/api/health':
            return self._send_json(200, {'status': 'online'})
        if (self.path.startswith('/api/transaction-a/jobs/') or self.path.startswith('/api/transaction-b/jobs/')
                or self.path.startswith('/api/client-transaction-a/jobs/') or self.path.startswith('/api/client-transaction-b/jobs/')):
            job_id = self.path.rsplit('/', 1)[-1]
            with JOBS_LOCK:
                job = JOBS.get(job_id)
            if not job:
                return self._send_json(404, {'error': 'Job not found'})
            return self._send_json(200, job)
        return self.serve_static()

    def do_POST(self):
        if self.path in ('/api/transaction-a/start', '/api/transaction-b/start'):
            try:
                config = self._read_json_body()
            except Exception as exc:
                return self._send_json(400, {'error': f'Invalid JSON body: {exc}'})
            route_key = 'transaction-a' if 'transaction-a' in self.path else 'transaction-b'
            meta = TRANSACTION_META[route_key]
            job_id = str(uuid.uuid4())
            job = init_job(job_id, meta)
            with JOBS_LOCK:
                JOBS[job_id] = job
            worker = threading.Thread(target=run_transaction, args=(job_id, config, meta), daemon=True)
            worker.start()
            return self._send_json(200, {'job_id': job_id})
        if self.path in ('/api/client-transaction-a/start', '/api/client-transaction-b/start'):
            try:
                config = self._read_json_body()
            except Exception as exc:
                return self._send_json(400, {'error': f'Invalid JSON body: {exc}'})
            route_key = 'client-transaction-a' if 'client-transaction-a' in self.path else 'client-transaction-b'
            meta = CLIENT_TRANSACTION_META[route_key]
            job_id = str(uuid.uuid4())
            seed_meta = {
                'steps': [('discover', 'Discover'), ('select_offer', 'Authorize'), ('wait_negotiation', 'Agreement'), ('transfer', 'Transfer'), ('wait_transfer', 'Prepare'), ('get_edr', 'Open Result'), ('download', 'Prepare Result')],
                'transaction_name': meta['transaction_name']
            }
            job = init_job(job_id, seed_meta)
            job['authorization_checks'] = {}
            with JOBS_LOCK:
                JOBS[job_id] = job
            target = run_client_transaction_a if route_key == 'client-transaction-a' else run_client_transaction_b
            worker = threading.Thread(target=target, args=(job_id, config), daemon=True)
            worker.start()
            return self._send_json(200, {'job_id': job_id})
        return self._send_json(404, {'error': 'Unknown API endpoint'})

    def serve_static(self):
        route = self.path.split('?', 1)[0]
        route = route.lstrip('/') or 'index.html'
        file_path = (BASE_DIR / route).resolve()
        if not str(file_path).startswith(str(BASE_DIR.resolve())):
            return self._send_json(403, {'error': 'Forbidden'})
        if file_path.is_dir():
            file_path = file_path / 'index.html'
        if not file_path.exists():
            return self._send_json(404, {'error': 'Not found'})
        content_type, _ = mimetypes.guess_type(str(file_path))
        data = file_path.read_bytes()
        self._send_bytes(200, data, content_type or 'application/octet-stream')


def main():
    server = ThreadingHTTPServer((HOST, PORT), AppHandler)
    print(f'Local HK EV platform is running at http://{HOST}:{PORT}')
    print('Keep your EDC / Docker / Kubernetes environment running in the background for live Transaction A / B execution.')
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == '__main__':
    main()
