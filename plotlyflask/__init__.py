"""Initialize Flask app."""
from flask import Flask
from flask_assets import Environment
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager

db = SQLAlchemy()
login_manager = LoginManager()

def init_app():
    """Construct core Flask application with embedded Dash app."""
    app = Flask(__name__, instance_relative_config=False)
    app.config.from_object('config.Config')
    assets = Environment()
    assets.init_app(app)

    # Initialize Plugins
    db.init_app(app)
    login_manager.init_app(app)

    with app.app_context():
        # Import parts of our core Flask app
        from . import routes
        from . import auth
        from .assets import compile_static_assets

        # Import Dash application
        from .plotlydash.dashboard_like import init_likeDashboard
        app = init_likeDashboard(app)
        from .plotlydash.dashboard_comment import init_commentDashboard
        app = init_commentDashboard(app)
        from .plotlydash.dashboard_response import init_responseDashboard
        app = init_responseDashboard(app)
        from .plotlydash.dashboard_post import init_postDashboard
        app = init_postDashboard(app)

        # Register Blueprints
        app.register_blueprint(routes.main_bp)
        app.register_blueprint(auth.auth_bp)

        # Create Database Models
        db.create_all()

        # Compile static assets
        compile_static_assets(assets)

        return app
