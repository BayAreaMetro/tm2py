"""Emme (Traffic and transit assginment) related shared code and core components.

"""
from socket import error as _socket_error
import inro.emme.desktop.app as _app
import inro.modeller as _m

# Cache running Emme projects from this process (simple singleton implementation)
_EMME_PROJECT_REF = {}


class EmmeProjectCache:
    """Centralized cache to keep same Emme project for traffic and transit assignments."""

    def __init__(self):
        self._project_cache = _EMME_PROJECT_REF

    def close_all(self):
        """
        Close open emme project(s).

        Should be called at the end of the model process / Emme assignments.
        """
        while self._project_cache:
            _, app = self._project_cache.popitem()
            app.close()

    def create_project(self, project_path, name):
        """Create, open and return Emme project"""
        project_path = _app.create_project(project_path, name)
        return self.project(project_path)

    def project(self, project_path):
        """Return already open Emme project, or open new Desktop session if not found."""
        emme_project = self._project_cache.get(project_path)
        if emme_project:
            try:  # Check if the Emme window was closed
                emme_project.current_window()
            except _socket_error:
                emme_project = None
        # if window is not opened in this process, start a new one
        if emme_project is None:
            emme_project = _app.start_dedicated(
                visible=True, user_initials="inro", project=project_path
            )
            self._project_cache[project_path] = emme_project
        return emme_project

    def modeller(self, emme_project):
        """Return Modeller object"""
        try:
            return _m.Modeller()
        except AssertionError:
            return _m.Modeller(emme_project)
