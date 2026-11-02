// Actual component regression; native hit testing is covered by flagship copy selection.
const assert = require('node:assert/strict');
const {test} = require('node:test');
const fs = require('node:fs');
const path = require('node:path');
const {createRequire} = require('node:module');
const frontend = path.resolve(__dirname, '../dash_tanstack_pivot');
const dependencies = createRequire(path.join(process.env.PIVOT_FRONTEND_TEST_DEPENDENCIES || frontend, 'package.json'));
const React = dependencies('react');
const {renderToStaticMarkup} = dependencies('react-dom/server');
const filename = path.join(frontend, 'src/lib/components/Notification.js');
const code = dependencies('@babel/core').transformSync(fs.readFileSync(filename, 'utf8'), {
    filename, presets: [[dependencies('@babel/preset-env'), {targets:{node:'current'}, modules:'commonjs'}],
        dependencies('@babel/preset-react')],
}).code;
const moduleUnderTest = {exports:{}};
new Function('module', 'exports', 'require', code)(moduleUnderTest, moduleUnderTest.exports, request => {
    if (request === 'react') return React;
    if (request === '../utils/Icons') return {Close:() => React.createElement('svg', {'aria-hidden':true})};
    return dependencies(request);
});
const Notification = moduleUnderTest.exports.default;

for (const type of ['success','info','warning','error']) {
    test(`${type} notification feedback lets native pointer input reach the workspace`, () => {
        const element = Notification({message:'Copied! <range>',type,onClose:() => {}});
        assert.equal(element.props.style.pointerEvents,'none',
            'A transient message must not intercept a second right-click on the selected cell beneath it');
        assert.equal(element.props.role,'status');
        assert.equal(element.props['aria-live'],'polite');
        const html = renderToStaticMarkup(element);
        assert.match(html,/Copied! &lt;range&gt;/,'Feedback remains visible to readers and assistive technology');
    });

    test(`${type} notification retains a native dismiss button`, () => {
        let closes = 0;
        const element = Notification({message:'Copied!',type,onClose:() => closes++});
        const dismiss = React.Children.toArray(element.props.children).find(child => child.type === 'button');
        assert.ok(dismiss,'Dismiss must support native pointer, Tab, Enter and Space activation');
        assert.equal(dismiss.props.type,'button','Dismiss must not submit an enclosing host form');
        assert.equal(dismiss.props['aria-label'],'Dismiss notification');
        assert.equal(dismiss.props.style.pointerEvents,'auto','Dismiss remains a pointer target');
        assert.notEqual(dismiss.props.tabIndex,-1);
        dismiss.props.onClick();
        assert.equal(closes,1);
    });
}
