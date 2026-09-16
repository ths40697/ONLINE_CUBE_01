# WebSocketのゲームサーバー

# このサーバーの役割
# 1. 複数のUnityクライアント(プレイヤー)からの接続を受け付ける
# 2. 各プレイヤーの座標を受け取り、他のプレイヤーに転送する
# 3. プレイヤーの参加と退出を管理する

# 非同期通信(複数のクライアント(プレイヤー)を同時に扱うため必要)
import asyncio
# JSON形式でデータを読み書きする
import json
# WebSocket通信を実現するライブラリ(requirements.txtでインストール)
import websockets

# グローバル変数(サーバー全体で共有する)
# 現在接続中のプレイヤーを保存する
players = {}

# 次に参加するプレイヤーに割り振るID
next_id = 1

# プレイヤー1人につき1つ動く関数
# 新しいプレイヤーが接続するたびにこの関数が実行される
# async defにすることで複数のプレイヤーが同時に接続しても並行して処理できる


async def handler(websocket):
    # 接続したプレイヤーにIDを割り振る
    # 関数の外にあるnext_idをグローバル変数として使うために必要
    global next_id

    # プレイヤーの参加処理
    # 新しいプレイヤーにIDを割り振って管理リストに追加する
    player_id = next_id
    # next_idを1つ更新
    next_id += 1
    # プレイヤーの情報を保存
    players[player_id] = {
        # メッセージを送るためのWebSocketオブジェクト
        "ws": websocket,
        # X座標(初期位置)
        "x": 0.0,
        # Z座標(初期位置)
        "z": 0.0
    }

    print(f"[接続] Player{player_id}が接続しました(現在{len(players)}人)")

    # 参加したプレイヤーに「あなたはPlayerooです」と伝える
    # また、すでに参加しているプレイヤーの座標も一緒に送る
    # 後から参加したプレイヤーが既存のプレイヤーのキューブを表示できるようにする
    await websocket.send(json.dumps({
        # メッセージの種類
        "type": "joined",
        # プレイヤーID
        "player_id": player_id,
        # 今参加している全プレイヤーの座標
        "players": {
            str(pid): {
                "x": p["x"],
                "z": p["z"]
            }
            # 全要素に対して、IDと座標を抜き取って新しいデータを作る
            for pid, p in players.items()
        }

    }))

    # 他のプレイヤーに「新しい人が来ました」と通知する
    await broadcast({
        # メッセージの種類
        "type": "player_joined",
        # 参加したプレイヤーのID
        "player_id": player_id,
        # 初期座標
        "x": 0.0,
        "z": 0.0
    },
        # 自分自身には送らない
        exclude=player_id)

    # メッセージ受信ループ
    # Unityからメッセージが届くたびに処理が実行される
    # 受け取るメッセージの内容は、プレイヤーが動いたときの座標
    try:
        async for raw_message in websocket:
            # 受け取ったメッセージをJson形式なので、Pythonの辞書型に変換する
            data = json.loads(raw_message)

            # typeフィールドによって処理を分ける
            if data["type"] == "move":
                # プレイヤーが移動した　-> 座標を更新して全プレイヤーに知らせる
                # サーバー側のプレイヤーの座標を更新
                players[player_id]["x"] = data["x"]
                players[player_id]["z"] = data["z"]
                # 全プレイヤーに移動を知らせる(自分には送らない)
                await broadcast({
                    # メッセージの種類
                    "type": "player_update",
                    # 移動したプレイヤーのID
                    "player_id": player_id,
                    # 新しいX座標
                    "x": data["x"],
                    # 新しいZ座標
                    "z": data["z"]

                }, exclude=player_id)
    # ゲームを閉じたり、ネットワークが切れたときに実行される
    except websockets.exceptions.ConnectionClosed:
        # エラーではなく、正常な終了なので何もしない
        pass
    # プレイヤー退出処理
    # tryとexceptのどちらでおわっても必ずここが実行される
    finally:
        # プレイヤー情報を削除
        del players[player_id]
        print(f"[切断]Player{player_id}が退出しました(現在{len(players)}人)")
        # 他のプレイヤーに「ooが退出しました」と通知する
        await broadcast({
            "type": "player_left",
            "player_id": player_id
        })


async def broadcast(message, exclude=None):
    # 誰も接続していない場合は処理が終了する
    # 後続の処理が無駄になるので、先に終わらせる
    # ガード節
    if not players:
        return
    # 辞書 -> json文字列に変換する
    data = json.dumps(message)
    # 全プレイヤーへの送信タスクを一覧にする
    tasks = [
        p["ws"].send(data)
        for pid, p in players.items()
        # 指定したプレイヤーには送らない
        if pid != exclude
    ]

    if tasks:
        # 全プレイヤーに一斉に送信する
        await asyncio.gather(*tasks)

# サーバーの起動関数


async def main():
    print("=" * 40)
    print(" オンラインゲーム WebSocket サーバー")
    print(" ws://0.0.0.0:8765")
    print("=" * 40)

    # 0.0.0.0はすべてのIPアドレスから接続を受け付けるという意味
    # localhost(127.0.0.1)だと自分のPCからしかつながらない
    async with websockets.serve(handler, "0.0.0.0", 8765):
        # この行で「ずっと通信し続けられる」状態になる
        await asyncio.Future()

# このファイルを直接実行したときだけmain()をを呼ぶ
# ほかのファイルからimportされたときには呼ばれない
if __name__ == "__main__":
    asyncio.run(main())
