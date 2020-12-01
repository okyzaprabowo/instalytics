"""Routes for parent Flask app."""
from flask import Blueprint, render_template, redirect, url_for
from flask import current_app as app
from flask_login import current_user, login_required, logout_user

# Blueprint Configuration
main_bp = Blueprint(
    'main_bp', __name__,
    template_folder='templates',
    static_folder='static'
)

@main_bp.route('/', methods=['GET'])
@login_required
def dashboard():
    """Logged-in User Dashboard."""
    return render_template(
        'dashboard.jinja2',
        title='Flask-Login Tutorial.',
        template='dashboard-template',
        current_user=current_user,
        body="You are now logged in!"
    )

@main_bp.route("/logout")
@login_required
def logout():
    """User log-out logic."""
    logout_user()
    return redirect(url_for('auth_bp.login'))


# @app.route('/')
# def home():
        # app = init_likeDashboard(app)
    # """Landing page."""
    # return render_template(
    #     'index.jinja2',
    #     title='Plotly Dash Flask Tutorial',
    #     description='Embed Plotly Dash into your Flask applications.',
    #     template='home-template',
    #     body="This is a homepage served with Flask."
    # )
    # return redirect('/likeDashboard')
