"""Plotly Dash HTML layout override."""

html_layout_original = '''
<!DOCTYPE html>
    <html>
        <head>
            {%metas%}
            <title>{%title%}</title>
            {%favicon%}
            {%css%}
        </head>
        <body class="dash-template">
            <header>
              <div class="nav-wrapper">
                <a href="/">
                    <img src="/static/img/logo.png" class="logo" />
                    <h1>Plotly Dash Flask Tutorial</h1>
                  </a>
                <nav>
                </nav>
            </div>
            </header>
            {%app_entry%}
            <footer>
                {%config%}
                {%scripts%}
                {%renderer%}
            </footer>
        </body>
    </html>
'''
html_layout = '''
<!DOCTYPE html>
<html>
        <head>
            {%metas%}
            <title>{%title%}</title>
            {%favicon%}
            {%css%}
        </head>
        <body class="dash-template">
            <header>
                <div class="nav-wrapper">
                    <a href="/">
                        <!-- <img src="/static/img/logo.png" class="logo" /> -->
                        <h1>Instalytics</h1>
                    </a>
                    <a href="/logout" style="text-align: right">Logout</a> 
                </div>
            </header>
            <div class="sidebar">
                <a href="/likeDashboard/">Like Analysis</a>
                <a href="/commentDashboard/">Comment Analysis</a>
                <a href="/responseDashboard/">Response Analysis</a>
                <a href="/postDashboard/">Post Analysis</a>
            </div>
            <div class="content">
                {%app_entry%}
            </div>
            <footer>
                {%config%}
                {%scripts%}
                {%renderer%}
            </footer>
        </body>
        </html>
'''