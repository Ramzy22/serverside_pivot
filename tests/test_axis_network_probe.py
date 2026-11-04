"""Execute the injected observer in Node; no browser or runtime replacement."""
import json
import shutil
import subprocess

import pytest

from tests.flagship_axis_swap import AXIS_PROBE


HARNESS = r"""
const assert = require('node:assert/strict');
const vm = require('node:vm');
const {probe, scenario} = JSON.parse(require('node:fs').readFileSync(0, 'utf8'));
const request = {schemaVersion:1, kind:'data', requestId:'req-1', state:{rowFields:['region']}, payload:{}};
const envelope = {...request, status:'error', payload:{code:'resource_exhausted',message:'Query capacity reached',secret:'SECRET'}};
if(scenario==='top-level-error') {
  envelope.message='Public runtime error';
  envelope.payload.resourceKind='query';
  envelope.payload.retryAfterMs=1501;
}
const calls = [];
let nativeResponse, nativeError;
class XHR extends EventTarget {
  open(...args) {this.openArgs=args; return 'opened';}
  send(...args) {calls.push([this,...args]); return 'sent';}
}
const window = {location:{href:'https://example.test/'}, fetch: (...args) => {
  calls.push([window,...args]);
  return nativeError ? Promise.reject(nativeError) : Promise.resolve(nativeResponse);
}};
vm.runInNewContext(probe, {window, XMLHttpRequest:XHR, URL, performance:{now:()=>7}});
const trace = window.__pivotAxisTrace;
const records = phase => trace.filter(item=>item.phase===phase);
const settle = async () => {for(let i=0;i<5;i++) await new Promise(resolve=>setImmediate(resolve));};
(async()=>{
  const url='https://user:SECRET@example.test/v1/runtime?token=SECRET#SECRET';
  const options={method:'POST',body:JSON.stringify(request)};
  if(scenario.startsWith('xhr-')) {
    const xhr=new XHR();
    assert.equal(xhr.open('POST',url,true),'opened');
    assert.deepEqual(xhr.openArgs,['POST',url,true]);
    assert.equal(xhr.send(options.body),'sent');
    assert.equal(calls[0][0],xhr);
    assert.equal(calls[0][1],options.body);
    xhr.status=scenario==='xhr-load'?503:0;
    xhr.responseText=JSON.stringify(envelope);
    xhr.dispatchEvent(new Event(scenario.slice(4)));
    const event=records('network-'+scenario)[0];
    assert.ok(event, 'XHR terminal event must be observable');
    assert.equal(event.httpStatus,xhr.status);
    if(scenario==='xhr-load') assert.equal(records('network-envelope-error')[0].code,'resource_exhausted');
  } else if(scenario==='rejection' || scenario==='abort') {
    nativeError=new Error('SECRET arbitrary network message');
    nativeError.name=scenario==='abort'?'AbortError':'TypeError';
    await assert.rejects(window.fetch(url,options),error=>error===nativeError);
    const event=records('network-fetch-rejection')[0];
    assert.ok(event,'Original fetch rejection must also be observed');
    assert.equal(event.name,nativeError.name);
  } else {
    nativeResponse=new Response(JSON.stringify(envelope),{status:scenario==='http-error'?503:200});
    if(scenario==='clone-failure') nativeResponse.clone=()=>{throw new Error('SECRET clone details');};
    if(scenario==='clone-read-failure') nativeResponse.clone=()=>({text:()=>Promise.reject(new Error('SECRET read details'))});
    const received=await window.fetch(url,options);
    assert.equal(received,nativeResponse,'Observer must return original response even if clone fails');
    assert.equal(await received.text(),JSON.stringify(envelope),'Original body must remain readable');
    await settle();
    assert.equal(records('network-http')[0]?.httpStatus,nativeResponse.status);
    if(scenario.startsWith('clone-')) assert.equal(records('network-clone-error').length,1);
    else {
      const error=records('network-envelope-error')[0];
      assert.ok(error,'HTTP 200 and 503 V1 error envelopes must be observed');
      assert.equal(error.code,'resource_exhausted');
      assert.equal(error.message,scenario==='top-level-error'?'Public runtime error':'Query capacity reached');
      if(scenario==='top-level-error') {
        assert.equal(error.resourceKind,'query');
        assert.equal(error.retryAfterMs,1501);
      }
      assert.equal(error.requestId,'req-1');
      assert.equal(error.httpStatus,nativeResponse.status);
      const legacy=records('response')[0];
      assert.equal(legacy.status,'error');
      assert.deepEqual(JSON.parse(JSON.stringify(legacy)), {
        phase:'response',at:7,requestId:'req-1',status:'error',layoutMode:null,
        rowFields:['region'],valueAggregations:null,
      });
    }
    assert.equal(calls.length,1);
    assert.equal(calls[0][1],url);
    assert.equal(calls[0][2],options);
  }
  assert.deepEqual(JSON.parse(JSON.stringify(records('request'))), [{
    phase:'request',at:7,requestId:'req-1',layoutMode:'hierarchy',
    rowFields:['region'],valueAggregations:null,
  }], 'Existing request trace must remain unchanged');
  const diagnostics=trace.filter(item=>item.phase.startsWith('network-'));
  assert.ok(diagnostics.length);
  assert.ok(!JSON.stringify(diagnostics).includes('SECRET'),'No URL secrets, arbitrary error text or unknown body fields');
  assert.ok(diagnostics.every(item=>item.endpoint==='/v1/runtime'));
})().catch(error=>{console.error(error);process.exitCode=1;});
"""


@pytest.mark.parametrize('scenario', [
    'http-error', 'http200-error', 'top-level-error', 'rejection', 'abort', 'clone-failure',
    'clone-read-failure', 'xhr-load', 'xhr-error', 'xhr-abort', 'xhr-timeout',
])
def test_axis_network_probe_observes_failures_without_changing_transport(scenario):
    node = shutil.which('node')
    if not node:
        pytest.skip('Node is required to execute the injected JavaScript observer')
    result = subprocess.run([node, '-e', HARNESS], input=json.dumps({'probe': AXIS_PROBE, 'scenario': scenario}),
                            capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stdout + result.stderr
