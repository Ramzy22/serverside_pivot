(function enablePivotProfiler() {
    // Dash loads assets before its renderer mounts the React component, so the
    // component sees profiling enabled on its first render and asks the server
    // to attach detailed callback/service/controller timings to every request.
    window.__PIVOT_PROFILE__ = true;
    window.__PIVOT_PROFILE_CONSOLE__ = true;
}());
