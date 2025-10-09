""" ----------------------------
UTFPR - Universidade Tecnológica Federal do Paraná
Curso: Sistemas de Informação 
Matéria: Sistemas Distribuídos

Aluno: Vitor Chiuco Zeni
---------------------------- """ 

import sys
import time
import threading
import subprocess
import Pyro5.api
import Pyro5.errors
from collections import deque

# --- Constantes e Configurações ---
PEER_NAMES = ["PeerA", "PeerB", "PeerC", "PeerD"]
HEARTBEAT_INTERVAL = 2
HEARTBEAT_TIMEOUT = 5
REQUEST_TIMEOUT = 5
RESOURCE_ACCESS_TIME = 10 
Pyro5.config.COMMTIMEOUT = 1.5

# --- Estados do Peer ---
STATE_RELEASED = "RELEASED"
STATE_WANTED = "WANTED"
STATE_HELD = "HELD"

def start_name_server():
    try:
        Pyro5.api.locate_ns()
        print("Servidor de Nomes do PyRO já está em execução.")
    except Pyro5.errors.NamingError:
        print("Iniciando o Servidor de Nomes do PyRO...")
        subprocess.Popen("pyro5-ns", shell=True)
        time.sleep(1)

@Pyro5.api.expose
@Pyro5.api.behavior(instance_mode="single")
class Peer:
    def __init__(self, name):
        self.name = name
        self.peer_uris = {} 
        self.state = STATE_RELEASED
        self.logical_clock = 0
        self.request_queue = deque()
        self.our_timestamp = -1
        self.replies_received = set()
        self.lock = threading.Lock()
        
        self.active_peers = set()
        self.last_heartbeat = {}
        
        self.resource_timer = None
        self.all_replies_event = threading.Event()

        print(f"[{self.name}] Peer inicializado. Estado: {self.state}")

    def discover_peers(self):
        print(f"[{self.name}] Procurando por outros peers...")
        ns = Pyro5.api.locate_ns()
        time.sleep(2)
        
        for peer_name in PEER_NAMES:
            if peer_name != self.name:
                try:
                    peer_uri = ns.lookup(peer_name)
                    self.peer_uris[peer_name] = peer_uri
                    self.active_peers.add(peer_name)
                    self.last_heartbeat[peer_name] = time.time()
                    print(f"[{self.name}] Encontrou {peer_name}.")
                except Pyro5.errors.NamingError:
                    print(f"[{self.name}] ERRO: Não foi possível encontrar {peer_name}.")
        print("-" * 30)
    
    def enter_critical_section(self):
        with self.lock:
            if self.state != STATE_HELD:
                return
            
            print(f"\n==============================================")
            print(f"[{self.name}] ACESSO CONCEDIDO À SEÇÃO CRÍTICA!")
            print(f"    -> Estado atual: {self.state}")
            print(f"    -> O recurso será liberado automaticamente em {RESOURCE_ACCESS_TIME} segundos.")
            print(f"==============================================\n")
            
            self.resource_timer = threading.Timer(RESOURCE_ACCESS_TIME, self.auto_release_resource)
            self.resource_timer.start()

    def auto_release_resource(self):
        print(f"\n[{self.name}] TEMPO ESGOTADO! Liberando o recurso!!!")
        self.release_resource(is_auto=True)

    def _send_message_to_peer(self, peer_name, method_name, *args):
        try:
            uri = self.peer_uris[peer_name]
            with Pyro5.api.Proxy(uri) as proxy:
                getattr(proxy, method_name)(*args)
        except (KeyError, Pyro5.errors.CommunicationError, Pyro5.errors.NamingError):
            self.handle_failed_peer(peer_name)

    def request_resource(self):
        with self.lock:
            if self.state != STATE_RELEASED:
                print(f"[{self.name}] Acesso já solicitado ou obtido. Estado atual: {self.state}")
                return
            self.state = STATE_WANTED
            print(f"[{self.name}] Estado alterado para: {self.state}")
            self.logical_clock += 1
            self.our_timestamp = self.logical_clock
            self.replies_received.clear()
            self.all_replies_event.clear()
            print(f"[{self.name}] Requisitando recurso com timestamp {self.our_timestamp}.")
        
        with self.lock:
            active_peers_snapshot = list(self.active_peers)

        if not active_peers_snapshot:
            print(f"[{self.name}] Nenhum outro peer ativo. Acesso concedido imediatamente.")
            self.state = STATE_HELD
            self.enter_critical_section()
            return

        for name in active_peers_snapshot:
            threading.Thread(target=self._send_message_to_peer, 
                            args=(name, 'receive_request', self.our_timestamp, self.name)).start()
        
        self.wait_for_replies()

    def wait_for_replies(self):
        with self.lock:
            num_peers_to_wait_for = len(self.active_peers)
        print(f"[{self.name}] Aguardando respostas de {num_peers_to_wait_for} peers...")
        
        event_is_set = self.all_replies_event.wait(timeout=REQUEST_TIMEOUT)

        if not event_is_set:
            print(f"[{self.name}] Timeout! Verificando peers que não responderam...")
            with self.lock:
                non_responsive_peers = self.active_peers - self.replies_received
                for peer_name in list(non_responsive_peers):
                    print(f"[{self.name}] Peer {peer_name} não respondeu a tempo.")
                    self.handle_failed_peer(peer_name, check_replies=False)
                
                if self.replies_received == self.active_peers:
                    self.state = STATE_HELD
                    self.enter_critical_section()
                else:
                    print(f"[{self.name}] Não foi possível obter acesso. Retornando ao estado RELEASED.")
                    self.state = STATE_RELEASED
        else:
            self.state = STATE_HELD
            self.enter_critical_section()

    def release_resource(self, is_auto=False):
        peers_to_reply = []
        with self.lock:
            if self.state != STATE_HELD:
                print(f"\n[{self.name}] Você não possui o recurso para liberar.")
                return

            if not is_auto:
                if self.resource_timer and self.resource_timer.is_alive():
                    self.resource_timer.cancel()
                    print(f"\n[{self.name}] Liberação manual. Timer de expiração cancelado.")

            self.state = STATE_RELEASED
            print(f"[{self.name}] Estado alterado para: {self.state}")
            self.our_timestamp = -1
            
            while self.request_queue:
                _timestamp, peer_name = self.request_queue.popleft()
                if peer_name in self.active_peers:
                    peers_to_reply.append(peer_name)
        
        if peers_to_reply:
            print(f"[{self.name}] Recurso liberado. Respondendo à fila de espera.")
            for peer_name in peers_to_reply:
                self._send_message_to_peer(peer_name, 'receive_reply', self.name)
        else:
            print(f"[{self.name}] Recurso liberado.")

    def list_active_peers(self):
        print("\n--- Peers Ativos ---")
        with self.lock:
            active_peers = list(self.active_peers)
        if not active_peers:
            print("Nenhum outro peer ativo no momento.")
        else:
            for peer_name in sorted(active_peers):
                print(f"- {peer_name}")
        print("--------------------")

    @Pyro5.api.oneway
    def receive_request(self, timestamp, peer_name):
        with self.lock:
            self.logical_clock = max(self.logical_clock, timestamp) + 1
            has_priority = (timestamp, peer_name) < (self.our_timestamp, self.name)

            if self.state == STATE_HELD or (self.state == STATE_WANTED and has_priority):
                self.request_queue.append((timestamp, peer_name))
                print(f"[{self.name}] Pedido de {peer_name} enfileirado.")
            else:
                if peer_name in self.active_peers:
                    self._send_message_to_peer(peer_name, 'receive_reply', self.name)

    @Pyro5.api.oneway
    def receive_reply(self, peer_name):
        with self.lock:
            if self.state == STATE_WANTED:
                self.replies_received.add(peer_name)
                if self.replies_received == self.active_peers:
                    self.all_replies_event.set()
    
    @Pyro5.api.oneway
    def receive_heartbeat(self, peer_name):
        with self.lock:
            self.last_heartbeat[peer_name] = time.time()

    def send_heartbeats(self):
        while True:
            time.sleep(HEARTBEAT_INTERVAL)
            with self.lock:
                active_peers_snapshot = list(self.active_peers)
            for name in active_peers_snapshot:
                self._send_message_to_peer(name, 'receive_heartbeat', self.name)
    
    def check_heartbeats(self):
        while True:
            time.sleep(HEARTBEAT_TIMEOUT)
            with self.lock:
                now = time.time()
                inactive_peers = [name for name, last_ts in self.last_heartbeat.items()
                                  if now - last_ts > HEARTBEAT_TIMEOUT and name in self.active_peers]
            
            for name in inactive_peers:
                print(f"\n[{self.name}] Timeout de heartbeat de {name}.")
                self.handle_failed_peer(name)
    
    def handle_failed_peer(self, peer_name, check_replies=True):
        with self.lock:
            if peer_name in self.active_peers:
                print(f"[{self.name}] REMOVENDO peer inativo: {peer_name}")
                self.active_peers.remove(peer_name)
                self.peer_uris.pop(peer_name, None)
                self.last_heartbeat.pop(peer_name, None)
                self.request_queue = deque([(ts, name) for ts, name in self.request_queue if name != peer_name])
                
                if check_replies and self.state == STATE_WANTED:
                    if self.replies_received == self.active_peers:
                        self.all_replies_event.set()

def main():
    if len(sys.argv) < 2 or sys.argv[1] not in PEER_NAMES:
        print(f"Uso: python {sys.argv[0]} <NomeDoPeer>")
        print(f"Nomes válidos: {', '.join(PEER_NAMES)}")
        return
    peer_name = sys.argv[1]
    if peer_name == PEER_NAMES[0]:
        start_name_server()
    else:
        time.sleep(2)
    daemon = Pyro5.api.Daemon()
    peer = Peer(peer_name)
    uri = daemon.register(peer)
    ns = Pyro5.api.locate_ns()
    ns.register(peer_name, uri)
    print(f"[{peer_name}] Registrado. URI: {uri}\n" + "-" * 30)
    peer.discover_peers()
    daemon_thread = threading.Thread(target=daemon.requestLoop, daemon=True)
    daemon_thread.start()
    heartbeat_sender_thread = threading.Thread(target=peer.send_heartbeats, daemon=True)
    heartbeat_sender_thread.start()
    heartbeat_checker_thread = threading.Thread(target=peer.check_heartbeats, daemon=True)
    heartbeat_checker_thread.start()
    print(f"[{peer_name}] Pronto e com mecanismos de tolerância a falhas ativados.\n" + "-" * 30)
    
    # Loop da Interface de Linha de Comando (CLI)
    try:
        while True:
            print("\n--- Menu ---")
            print("1. Requisitar recurso")
            print("2. Liberar recurso")
            print("3. Listar peers ativos")
            choice = input("Escolha uma opção: ")

            if choice == '1':
                peer.request_resource()
            elif choice == '2':
                peer.release_resource()
            elif choice == '3':
                peer.list_active_peers()
            else:
                print("Opção inválida. Tente novamente.")

    except (KeyboardInterrupt, EOFError):
        print(f"\n[{peer_name}] Desligando...")
        if peer.resource_timer and peer.resource_timer.is_alive():
            peer.resource_timer.cancel()
        ns.remove(peer_name)
        daemon.shutdown()

if __name__ == "__main__":
    main()