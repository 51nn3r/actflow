import hashlib

from actflow import Task, TaskResult, SyncExecutor, OrderedInputController


ARRIVALS = [
    {"idx": 1, "user": "user2", "text": "второе по счёту сообщение"},
    {"idx": 0, "user": "user1", "text": "первое по счёту сообщение"},
]


class Feed(Task):
    """Puts each message on the wire in arrival order, tagged with its sequence idx."""

    def execute(self, batch: list) -> list[TaskResult]:
        for message in batch:
            print(f"прибыло: {message['user']} (idx={message['idx']})")

        return [self.to("msg", message) for message in batch]


class Md5(Task):
    """Hashes one message per tick; the ordered input releases them by ascending idx."""

    def execute(self, msg: dict) -> dict:
        digest = hashlib.md5(msg["text"].encode()).hexdigest()
        print(f"  [md5] считаю для idx={msg['idx']} ({msg['user']}) -> {digest[:8]}")
        return {None: (msg["idx"], digest)}


def build() -> Feed:
    feed = Feed()()
    md5 = Md5(input_controller=OrderedInputController(("msg",)))()
    feed["msg"] >> md5
    return feed


def main() -> None:
    print("протокол сообщений: второй пользователь ввёл раньше первого")
    result = SyncExecutor().run(build(), ARRIVALS)
    print("хеши (по возрастанию idx):", result)
    print("порядок сохранён:", result == sorted(result))


if __name__ == "__main__":
    main()
