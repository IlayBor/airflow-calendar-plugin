import os

from flask_appbuilder import BaseView, expose
from airflow.models import DagModel, DagRun
from airflow.models.dagbag import DagBag
from airflow.utils.session import provide_session

from airflow_calendar.utils import build_calendar_events


CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))


class CalendarView(BaseView):
    default_view = "index"
    template_folder = os.path.join(CURRENT_DIR, 'templates')
    route_base = "/airflow_calendar"
    base_permissions = ['can_list', 'menu_access']

    @expose("/")
    @provide_session
    def index(self, session=None):
        date_col = DagRun.execution_date
        date_attr = 'execution_date'

        query = session.query(DagModel).filter(DagModel.is_paused == False)
        if hasattr(DagModel, 'is_active'):
            query = query.filter(DagModel.is_active == True)

        dags = query.all()
        dagbag = DagBag(read_dags_from_db=True)
        events = build_calendar_events(
            session, dags, dagbag, date_col, date_attr)

        return self.render_template("calendar.html", events=events)
