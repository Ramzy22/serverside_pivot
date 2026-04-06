from dash_presentation.app import app, get_adapter
get_adapter()
app.run(debug=True, use_reloader=False, port=8051)
