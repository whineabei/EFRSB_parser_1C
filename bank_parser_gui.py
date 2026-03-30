from __future__ import annotations

import locale
import re
import sys
import ctypes
from pathlib import Path

import io
from contextlib import redirect_stderr, redirect_stdout

from PyQt6.QtCore import QObject, QProcess, QProcessEnvironment, QThread, QTimer, Qt, pyqtSignal
from PyQt6.QtGui import QFont, QIcon, QPixmap, QTextCursor
from PyQt6.QtWidgets import (
    QApplication,
    QFileDialog,
    QFrame,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QPlainTextEdit,
    QProgressBar,
    QPushButton,
    QSpinBox,
    QVBoxLayout,
    QWidget,
)


def get_base_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


def get_resource_dir() -> Path:
    return Path(getattr(sys, "_MEIPASS", get_base_dir()))


BASE_DIR = get_base_dir()
RESOURCE_DIR = get_resource_dir()
PARSER_SCRIPT = RESOURCE_DIR / "bank_parser.py"
ICON_PATH = RESOURCE_DIR / "images.ico"
PROGRESS_RE = re.compile(r"^\[(\d+)/(\d+)\]")
DONE_RE = re.compile(r"^\[done\]\s+(.*)$")
WORKER_FLAG = "--run-parser-worker"


def decode_output(raw: bytes) -> str:
    for encoding in ("utf-8", locale.getpreferredencoding(False), "cp1251"):
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace")


class SignalStream(io.TextIOBase):
    def __init__(self, callback) -> None:
        super().__init__()
        self.callback = callback
        self._buffer = ""

    def write(self, text: str) -> int:
        if not text:
            return 0
        self._buffer += text
        while "\n" in self._buffer:
            line, self._buffer = self._buffer.split("\n", 1)
            self.callback(line.rstrip("\r"))
        return len(text)

    def flush(self) -> None:
        if self._buffer:
            self.callback(self._buffer.rstrip("\r"))
            self._buffer = ""


class ParserWorker(QObject):
    log_line = pyqtSignal(str)
    finished = pyqtSignal(int)

    def __init__(self, parser_args: list[str]) -> None:
        super().__init__()
        self.parser_args = parser_args

    def run(self) -> None:
        import bank_parser

        original_argv = sys.argv[:]
        stream = SignalStream(self.log_line.emit)
        sys.argv = [str(PARSER_SCRIPT), *self.parser_args]
        exit_code = 0
        try:
            with redirect_stdout(stream), redirect_stderr(stream):
                exit_code = bank_parser.main()
        except KeyboardInterrupt:
            stream.write("[stop] Прервано пользователем\n")
            exit_code = 130
        except Exception as error:
            stream.write(f"[error] {error}\n")
            exit_code = 1
        finally:
            stream.flush()
            sys.argv = original_argv
            self.finished.emit(exit_code)


class StatCard(QFrame):
    def __init__(self, title: str, accent: str) -> None:
        super().__init__()
        self.setObjectName("statCard")
        layout = QVBoxLayout(self)
        layout.setContentsMargins(12, 10, 12, 10)
        layout.setSpacing(4)

        self.title_label = QLabel(title)
        self.title_label.setObjectName("cardTitle")

        self.value_label = QLabel("0")
        self.value_label.setObjectName("cardValue")
        self.value_label.setStyleSheet(f"color: {accent};")

        layout.addWidget(self.title_label)
        layout.addWidget(self.value_label)

    def set_value(self, value: str) -> None:
        self.value_label.setText(value)


class ParserWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.process: QProcess | None = None
        self.worker_thread: QThread | None = None
        self.worker: ParserWorker | None = None
        self.output_path: Path | None = None
        self.processed_cases = 0
        self.total_cases = 0
        self.elapsed_seconds = 0

        self.setWindowTitle("ЕФРСБ парсер -> 1С")
        self.resize(700, 500)
        self.setMinimumSize(640, 700)
        if ICON_PATH.exists():
            self.setWindowIcon(QIcon(str(ICON_PATH)))

        self.timer = QTimer(self)
        self.timer.setInterval(1000)
        self.timer.timeout.connect(self._tick)

        self.title_label = QLabel("ЕФРСБ парсер -> 1С")
        self.title_label.setObjectName("heroTitle")

        self.subtitle_label = QLabel("Загрузка Excel, запуск парсинга и просмотр логов в одном окне.")
        self.subtitle_label.setObjectName("heroSubtitle")
        self.subtitle_label.setWordWrap(True)
        self.icon_label = QLabel()
        self.icon_label.setObjectName("heroIcon")
        self.icon_label.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignTop)
        if ICON_PATH.exists():
            pixmap = QPixmap(str(ICON_PATH))
            self.icon_label.setPixmap(
                pixmap.scaled(
                    36,
                    36,
                    Qt.AspectRatioMode.KeepAspectRatio,
                    Qt.TransformationMode.SmoothTransformation,
                )
            )

        self.file_input = QLineEdit(str(self._default_input_path()))
        self.file_input.setPlaceholderText("Выберите Excel-файл для обработки")
        self.file_input.setMinimumHeight(38)

        self.browse_button = QPushButton("Выбрать файл")
        self.browse_button.clicked.connect(self._choose_file)
        self.browse_button.setMinimumHeight(38)

        self.run_button = QPushButton("Запустить парсер")
        self.run_button.clicked.connect(self._start_parser)
        self.run_button.setObjectName("primaryButton")
        self.run_button.setMinimumHeight(38)

        self.stop_button = QPushButton("Остановить")
        self.stop_button.clicked.connect(self._stop_parser)
        self.stop_button.setObjectName("secondaryButton")
        self.stop_button.setMinimumHeight(38)
        self.stop_button.setEnabled(False)

        self.limit_input = QSpinBox()
        self.limit_input.setRange(0, 100000)
        self.limit_input.setValue(0)
        self.limit_input.setSpecialValueText("Без лимита")
        self.limit_input.setMinimumHeight(36)

        self.status_pill = QLabel("Ожидание запуска")
        self.status_pill.setObjectName("statusIdle")
        self.status_pill.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.status_pill.setMinimumHeight(28)

        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        self.progress_bar.setFormat("%p%")
        self.progress_bar.setMinimumHeight(12)
        self.progress_bar.setTextVisible(False)

        self.progress_caption = QLabel("Прогресс обработки")
        self.progress_caption.setObjectName("sectionLabel")

        self.log_console = QPlainTextEdit()
        self.log_console.setReadOnly(True)
        self.log_console.setLineWrapMode(QPlainTextEdit.LineWrapMode.NoWrap)
        self.log_console.setObjectName("logConsole")

        self.status_card = StatCard("Статус", "#126a4b")
        self.time_card = StatCard("Время работы", "#0c6ddf")
        self.processed_card = StatCard("Обработано дел", "#9a4d00")
        self.total_card = StatCard("Всего дел", "#5b4bff")

        self.status_card.set_value("Ожидание")
        self.time_card.set_value("00:00:00")
        self.processed_card.set_value("0")
        self.total_card.set_value("0")

        central = QWidget()
        self.setCentralWidget(central)

        root = QVBoxLayout(central)
        root.setContentsMargins(14, 14, 14, 14)
        root.setSpacing(10)

        hero = QFrame()
        hero.setObjectName("heroCard")
        hero_layout = QVBoxLayout(hero)
        hero_layout.setContentsMargins(16, 14, 16, 14)
        hero_layout.setSpacing(4)
        hero_header = QHBoxLayout()
        hero_header.setContentsMargins(0, 0, 0, 0)
        hero_header.setSpacing(8)
        hero_header.addWidget(self.title_label, 1)
        hero_header.addWidget(self.icon_label)
        hero_layout.addLayout(hero_header)
        hero_layout.addWidget(self.subtitle_label)
        root.addWidget(hero)

        controls = QFrame()
        controls.setObjectName("panel")
        controls_layout = QVBoxLayout(controls)
        controls_layout.setContentsMargins(14, 14, 14, 14)
        controls_layout.setSpacing(10)

        file_row = QHBoxLayout()
        file_row.setSpacing(8)
        file_row.addWidget(self.file_input, 1)
        file_row.addWidget(self.browse_button)
        controls_layout.addLayout(file_row)

        settings_row = QHBoxLayout()
        settings_row.setSpacing(8)
        limit_label = QLabel("Лимит строк")
        limit_label.setObjectName("fieldLabel")
        settings_row.addWidget(limit_label)
        settings_row.addWidget(self.limit_input)
        settings_row.addStretch(1)
        settings_row.addWidget(self.status_pill)
        controls_layout.addLayout(settings_row)

        action_row = QHBoxLayout()
        action_row.setSpacing(8)
        action_row.addWidget(self.run_button)
        action_row.addWidget(self.stop_button)
        controls_layout.addLayout(action_row)

        root.addWidget(controls)

        stats_layout = QGridLayout()
        stats_layout.setHorizontalSpacing(8)
        stats_layout.setVerticalSpacing(8)
        stats_layout.addWidget(self.status_card, 0, 0)
        stats_layout.addWidget(self.time_card, 0, 1)
        stats_layout.addWidget(self.processed_card, 0, 2)
        stats_layout.addWidget(self.total_card, 0, 3)
        root.addLayout(stats_layout)

        root.addWidget(self.progress_caption)
        root.addWidget(self.progress_bar)

        logs_panel = QFrame()
        logs_panel.setObjectName("panel")
        logs_layout = QVBoxLayout(logs_panel)
        logs_layout.setContentsMargins(14, 12, 14, 14)
        logs_layout.setSpacing(8)
        logs_label = QLabel("Журнал выполнения")
        logs_label.setObjectName("sectionLabel")
        logs_layout.addWidget(logs_label)
        logs_layout.addWidget(self.log_console, 1)
        root.addWidget(logs_panel, 1)

        self._apply_styles()

    def _apply_styles(self) -> None:
        font = QFont("Segoe UI", 10)
        QApplication.instance().setFont(font)
        self.setStyleSheet(
            """
            QMainWindow, QWidget {
                background: #f3f6fb;
                color: #162033;
            }
            QFrame#heroCard {
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1,
                    stop:0 #16324f, stop:1 #245a87);
                border-radius: 18px;
            }
            QFrame#panel, QFrame#statCard {
                background: #ffffff;
                border: 1px solid #d9e3f0;
                border-radius: 16px;
            }
            QLabel#heroTitle {
                background: transparent;
                color: #ffffff;
                font-size: 18px;
                font-weight: 700;
            }
            QLabel#heroSubtitle {
                background: transparent;
                color: rgba(255, 255, 255, 0.82);
                font-size: 11px;
            }
            QLabel#sectionLabel {
                font-size: 14px;
                font-weight: 700;
                color: #243247;
            }
            QLabel#fieldLabel, QLabel#cardTitle {
                color: #66758c;
                font-size: 11px;
                font-weight: 600;
                text-transform: uppercase;
            }
            QLabel#cardValue {
                font-size: 18px;
                font-weight: 700;
            }
            QLabel#statusIdle, QLabel#statusRunning, QLabel#statusDone, QLabel#statusError {
                padding: 4px 10px;
                border-radius: 12px;
                font-size: 11px;
                font-weight: 700;
            }
            QLabel#statusIdle {
                background: #e9eef6;
                color: #4d5f79;
            }
            QLabel#statusRunning {
                background: #dff1ff;
                color: #0c6ddf;
            }
            QLabel#statusDone {
                background: #dff6ea;
                color: #126a4b;
            }
            QLabel#statusError {
                background: #ffe4e1;
                color: #b53d2e;
            }
            QLineEdit, QSpinBox, QPlainTextEdit {
                background: #f9fbfe;
                border: 1px solid #d6e0ec;
                border-radius: 12px;
                padding: 8px 10px;
                selection-background-color: #cfe5ff;
            }
            QLineEdit:focus, QSpinBox:focus, QPlainTextEdit:focus {
                border: 1px solid #4c91ff;
            }
            QPushButton {
                border: none;
                border-radius: 12px;
                padding: 8px 12px;
                font-weight: 700;
            }
            QPushButton#primaryButton {
                background: #1f6feb;
                color: #ffffff;
            }
            QPushButton#primaryButton:hover {
                background: #195ec8;
            }
            QPushButton#secondaryButton {
                background: #e8edf5;
                color: #314056;
            }
            QPushButton#secondaryButton:hover {
                background: #dbe4f0;
            }
            QPushButton:disabled {
                background: #cfd8e3;
                color: #7d8aa0;
            }
            QProgressBar {
                background: #dfe8f3;
                border: none;
                border-radius: 6px;
            }
            QProgressBar::chunk {
                background: qlineargradient(x1:0, y1:0, x2:1, y2:0,
                    stop:0 #1f6feb, stop:1 #37b38a);
                border-radius: 6px;
            }
            QPlainTextEdit#logConsole {
                background: #101722;
                color: #d7e3f4;
                border: 1px solid #1d2a3c;
                font-family: Consolas;
                font-size: 12px;
            }
            """
        )

    def _default_input_path(self) -> Path:
        for candidate in sorted(BASE_DIR.glob("*.xlsx")):
            if not candidate.name.startswith("~$"):
                return candidate
        return BASE_DIR / ""

    def _choose_file(self) -> None:
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Выберите Excel-файл",
            str(BASE_DIR),
            "Excel Files (*.xlsx *.xlsm *.xltx *.xltm)",
        )
        if file_path:
            self.file_input.setText(file_path)

    def _set_status(self, text: str, state: str) -> None:
        mapping = {
            "idle": "statusIdle",
            "running": "statusRunning",
            "done": "statusDone",
            "error": "statusError",
        }
        self.status_pill.setObjectName(mapping[state])
        self.status_pill.setText(text)
        self.status_pill.style().unpolish(self.status_pill)
        self.status_pill.style().polish(self.status_pill)
        self.status_card.set_value(text)

    def _start_parser(self) -> None:
        input_path = Path(self.file_input.text().strip())
        if not input_path.exists():
            QMessageBox.warning(self, "Файл не найден", "Выберите существующий Excel-файл.")
            return
        if not PARSER_SCRIPT.exists():
            QMessageBox.critical(self, "Ошибка", f"Не найден файл парсера: {PARSER_SCRIPT}")
            return
        if self.worker_thread and self.worker_thread.isRunning():
            self.append_log("[gui] Во встроенном режиме остановка сейчас не поддерживается. Закройте окно, если нужно прервать выполнение.")
            return
        if self.process and self.process.state() != QProcess.ProcessState.NotRunning:
            return

        self.output_path = input_path.with_name(f"{input_path.stem}_result.xlsx")
        self.processed_cases = 0
        self.total_cases = 0
        self.elapsed_seconds = 0
        self.progress_bar.setValue(0)
        self.time_card.set_value("00:00:00")
        self.processed_card.set_value("0")
        self.total_card.set_value("0")
        self._set_status("Запуск", "running")
        self.log_console.clear()

        if getattr(sys, "frozen", False):
            parser_args = ["--input", str(input_path)]
            if self.limit_input.value() > 0:
                parser_args.extend(["--limit", str(self.limit_input.value())])

            self.worker_thread = QThread(self)
            self.worker = ParserWorker(parser_args)
            self.worker.moveToThread(self.worker_thread)
            self.worker_thread.started.connect(self.worker.run)
            self.worker.log_line.connect(self._consume_text)
            self.worker.finished.connect(self._worker_finished)
            self.worker.finished.connect(self.worker_thread.quit)
            self.worker.finished.connect(self.worker.deleteLater)
            self.worker_thread.finished.connect(self.worker_thread.deleteLater)
            self.worker_thread.start()
        else:
            program = sys.executable
            args = ["-u", str(PARSER_SCRIPT), "--input", str(input_path)]
            if self.limit_input.value() > 0:
                args.extend(["--limit", str(self.limit_input.value())])

            self.process = QProcess(self)
            self.process.setWorkingDirectory(str(BASE_DIR))
            self.process.setProcessChannelMode(QProcess.ProcessChannelMode.SeparateChannels)
            env = QProcessEnvironment.systemEnvironment()
            env.insert("PYTHONIOENCODING", "utf-8")
            self.process.setProcessEnvironment(env)
            self.process.readyReadStandardOutput.connect(self._read_stdout)
            self.process.readyReadStandardError.connect(self._read_stderr)
            self.process.finished.connect(self._process_finished)
            self.process.errorOccurred.connect(self._process_error)
            self.process.start(program, args)

        self.run_button.setEnabled(False)
        self.stop_button.setEnabled(True)
        self.browse_button.setEnabled(False)
        self.file_input.setEnabled(False)
        self.limit_input.setEnabled(False)
        self._set_status("Выполняется", "running")
        self.timer.start()

    def _stop_parser(self) -> None:
        if self.process and self.process.state() != QProcess.ProcessState.NotRunning:
            self.append_log("[gui] Остановка процесса...")
            self.process.kill()

    def _read_stdout(self) -> None:
        if not self.process:
            return
        text = decode_output(bytes(self.process.readAllStandardOutput()))
        self._consume_text(text)

    def _read_stderr(self) -> None:
        if not self.process:
            return
        text = decode_output(bytes(self.process.readAllStandardError()))
        self._consume_text(text)

    def _consume_text(self, text: str) -> None:
        for line in text.splitlines():
            clean_line = line.rstrip()
            if not clean_line:
                continue
            self.append_log(clean_line)
            self._parse_progress(clean_line)

    def _parse_progress(self, line: str) -> None:
        progress_match = PROGRESS_RE.match(line)
        if progress_match:
            self.processed_cases = int(progress_match.group(1))
            self.total_cases = int(progress_match.group(2))
            self.processed_card.set_value(str(self.processed_cases))
            self.total_card.set_value(str(self.total_cases))
            if self.total_cases > 0:
                percent = int(self.processed_cases * 100 / self.total_cases)
                self.progress_bar.setValue(percent)
            return

        done_match = DONE_RE.match(line)
        if done_match:
            self.output_path = Path(done_match.group(1).split(":", 1)[-1].strip())

    def _process_finished(self, exit_code: int, _exit_status: QProcess.ExitStatus) -> None:
        self.process = None
        self.timer.stop()
        self.run_button.setEnabled(True)
        self.stop_button.setEnabled(False)
        self.browse_button.setEnabled(True)
        self.file_input.setEnabled(True)
        self.limit_input.setEnabled(True)

        if exit_code == 0:
            self._set_status("Завершено", "done")
            if self.total_cases:
                self.progress_bar.setValue(100)
            result_message = "Парсинг завершён."
            if self.output_path:
                result_message += f"\nРезультат: {self.output_path}"
            QMessageBox.information(self, "Готово", result_message)
        else:
            self._set_status("Ошибка", "error")
            QMessageBox.warning(self, "Ошибка", f"Процесс завершился с кодом {exit_code}.")

    def _process_error(self, error: QProcess.ProcessError) -> None:
        self.append_log(f"[gui] Ошибка запуска процесса: {error}")
        self._set_status("Ошибка запуска", "error")

    def _worker_finished(self, exit_code: int) -> None:
        self.worker = None
        self.worker_thread = None
        self.timer.stop()
        self.run_button.setEnabled(True)
        self.stop_button.setEnabled(False)
        self.browse_button.setEnabled(True)
        self.file_input.setEnabled(True)
        self.limit_input.setEnabled(True)

        if exit_code == 0:
            self._set_status("Завершено", "done")
            if self.total_cases:
                self.progress_bar.setValue(100)
            result_message = "Парсинг завершён."
            if self.output_path:
                result_message += f"\nРезультат: {self.output_path}"
            QMessageBox.information(self, "Готово", result_message)
        else:
            self._set_status("Ошибка", "error")
            QMessageBox.warning(self, "Ошибка", f"Процесс завершился с кодом {exit_code}.")

    def _tick(self) -> None:
        self.elapsed_seconds += 1
        hours, remainder = divmod(self.elapsed_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        self.time_card.set_value(f"{hours:02d}:{minutes:02d}:{seconds:02d}")

    def append_log(self, message: str) -> None:
        self.log_console.appendPlainText(message)
        cursor = self.log_console.textCursor()
        cursor.movePosition(QTextCursor.MoveOperation.End)
        self.log_console.setTextCursor(cursor)
        self.log_console.ensureCursorVisible()

    def closeEvent(self, event) -> None:  # type: ignore[override]
        if self.process and self.process.state() != QProcess.ProcessState.NotRunning:
            self.process.kill()
            self.process.waitForFinished(3000)
        super().closeEvent(event)


def run_parser_worker() -> int:
    worker_args = [arg for arg in sys.argv[1:] if arg != WORKER_FLAG]
    import bank_parser

    original_argv = sys.argv[:]
    sys.argv = [str(PARSER_SCRIPT), *worker_args]
    try:
        return bank_parser.main()
    except KeyboardInterrupt:
        print("\n[stop] Прервано пользователем")
        return 130
    except Exception as error:
        print(f"[error] {error}", file=sys.stderr)
        return 1
    finally:
        sys.argv = original_argv


def main() -> int:
    if WORKER_FLAG in sys.argv:
        return run_parser_worker()

    if sys.platform.startswith("win"):
        try:
            ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID(
                "bank.parser.gui"
            )
        except Exception:
            pass

    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    if ICON_PATH.exists():
        app.setWindowIcon(QIcon(str(ICON_PATH)))
    window = ParserWindow()
    window.show()
    return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())
