import sys
from typing import List, Optional

from PyQt6.QtCore import QThread, pyqtSignal
from PyQt6.QtWidgets import (
    QApplication,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

import vk_link_rewriter as core


class StreamToSignal:
    """Поток для перенаправления print() в сигнал Qt."""

    def __init__(self, signal):
        self.signal = signal

    def write(self, text: str) -> None:
        text = str(text)
        if not text:
            return
        self.signal.emit(text)

    def flush(self) -> None:
        # Ничего делать не нужно, но метод должен быть
        pass


class Worker(QThread):
    log = pyqtSignal(str)
    finished = pyqtSignal()
    error = pyqtSignal(str)

    def __init__(
        self,
        token: str,
        old_link: str,
        new_link: str,
        communities: List[str],
        parent=None,
    ) -> None:
        super().__init__(parent)
        self.token = token
        self.old_link = old_link
        self.new_link = new_link
        self.communities = communities
        self._stopped = False

    def stop(self) -> None:
        self._stopped = True

    def run(self) -> None:
        logger = StreamToSignal(self.log)
        old_stdout = sys.stdout
        old_stderr = sys.stderr
        sys.stdout = logger
        sys.stderr = logger

        try:
            try:
                core.init_vk_api(token=self.token or None, ignore_env_token=True)
            except Exception as e:
                self.error.emit(str(e))
                return

            if not self.communities:
                self.error.emit("Список сообществ пуст.")
                return

            for comm in self.communities:
                if self._stopped:
                    self.log.emit("\nОперация остановлена пользователем.\n")
                    break
                try:
                    core.process_community(comm, self.old_link, self.new_link)
                except Exception as e:
                    self.error.emit(f"Ошибка при обработке {comm}: {e}")

            self.finished.emit()
        finally:
            sys.stdout = old_stdout
            sys.stderr = old_stderr


class MainWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("VK Link Rewriter")

        self.worker: Optional[Worker] = None

        self._init_ui()

    def _init_ui(self) -> None:
        central = QWidget()
        self.setCentralWidget(central)

        layout = QVBoxLayout(central)

        layout.addWidget(QLabel("VK токен *:"))
        self.token_edit = QLineEdit()
        self.token_edit.setEchoMode(QLineEdit.EchoMode.Password)
        self.token_edit.setPlaceholderText("Токен с правами wall, groups, offline")
        layout.addWidget(self.token_edit)

        # Старая / новая ссылка
        layout.addWidget(QLabel("Старая ссылка (что искать):"))
        self.old_link_edit = QLineEdit()
        layout.addWidget(self.old_link_edit)

        layout.addWidget(QLabel("Новая ссылка (на что заменить):"))
        self.new_link_edit = QLineEdit()
        layout.addWidget(self.new_link_edit)

        # Сообщества
        layout.addWidget(QLabel("Ссылки или ID сообществ (по одной в строке):"))
        self.communities_edit = QTextEdit()
        self.communities_edit.setPlaceholderText("https://vk.com/public123\nhttps://vk.com/club123\nmy_public_name\n...")
        layout.addWidget(self.communities_edit)

        # Кнопки
        btn_layout = QHBoxLayout()
        self.start_btn = QPushButton("Запустить")
        self.stop_btn = QPushButton("Остановить")
        self.stop_btn.setEnabled(False)
        btn_layout.addWidget(self.start_btn)
        btn_layout.addWidget(self.stop_btn)
        layout.addLayout(btn_layout)

        # Лог
        layout.addWidget(QLabel("Лог:"))
        self.log_view = QTextEdit()
        self.log_view.setReadOnly(True)
        layout.addWidget(self.log_view)

        self.start_btn.clicked.connect(self.on_start_clicked)
        self.stop_btn.clicked.connect(self.on_stop_clicked)

    def append_log(self, text: str) -> None:
        self.log_view.moveCursor(self.log_view.textCursor().MoveOperation.End)
        self.log_view.insertPlainText(text)
        self.log_view.moveCursor(self.log_view.textCursor().MoveOperation.End)

    def on_start_clicked(self) -> None:
        if self.worker and self.worker.isRunning():
            return

        token = self.token_edit.text().strip()
        old_link = self.old_link_edit.text().strip()
        new_link = self.new_link_edit.text().strip()
        communities = [
            line.strip()
            for line in self.communities_edit.toPlainText().splitlines()
            if line.strip()
        ]

        if not token:
            QMessageBox.warning(self, "Ошибка", "Укажите VK токен.")
            return
        if not old_link or not new_link:
            QMessageBox.warning(self, "Ошибка", "Старая и новая ссылки не должны быть пустыми.")
            return
        if not communities:
            QMessageBox.warning(self, "Ошибка", "Укажите хотя бы одно сообщество.")
            return

        self.log_view.clear()
        self.append_log("Начало обработки...\n")

        self.worker = Worker(token, old_link, new_link, communities)
        self.worker.log.connect(self.append_log)
        self.worker.error.connect(self.on_worker_error)
        self.worker.finished.connect(self.on_worker_finished)

        self.start_btn.setEnabled(False)
        self.stop_btn.setEnabled(True)

        self.worker.start()

    def on_stop_clicked(self) -> None:
        if self.worker and self.worker.isRunning():
            self.worker.stop()
            self.stop_btn.setEnabled(False)

    def on_worker_error(self, message: str) -> None:
        QMessageBox.warning(self, "Ошибка", message)
        self.append_log(f"\nОшибка: {message}\n")

    def on_worker_finished(self) -> None:
        self.append_log("\nГотово.\n")
        self.start_btn.setEnabled(True)
        self.stop_btn.setEnabled(False)


def main() -> None:
    app = QApplication(sys.argv)
    window = MainWindow()
    window.resize(900, 600)
    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()

