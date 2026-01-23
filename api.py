"""
API para Papayas Tierlist - Versión Mínima que FUNCIONA
"""

from flask import Flask, jsonify
from flask_cors import CORS
import os
import psycopg2

app = Flask(__name__)
CORS(app)

def get_db_connection():
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        return None
    try:
        return psycopg2.connect(database_url)
    except:
        return None

@app.route('/')
def home():
    return jsonify({
        'status': 'online',
        'message': 'Papayas Tierlist API',
        'endpoints': {
            '/api/rankings/<mode>': 'Get rankings',
            '/api/player/<id>': 'Get player',
            '/api/stats': 'Get stats',
            '/health': 'Health check'
        }
    })

@app.route('/health')
def health():
    conn = get_db_connection()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM resultados")
            count = cur.fetchone()[0]
            conn.close()
            return jsonify({'status': 'ok', 'database': 'connected', 'total_tests': count})
        except:
            if conn:
                conn.close()
            return jsonify({'status': 'error', 'database': 'error'}), 500
    return jsonify({'status': 'error', 'database': 'disconnected'}), 500

@app.route('/api/rankings/<mode>')
def get_rankings(mode):
    """Rankings - VERSIÓN SIMPLE QUE FUNCIONA"""
    
    conn = get_db_connection()
    if not conn:
        return jsonify({
            'mode': mode,
            'players': [],
            'tiers': {},
            'total_players': 0
        })
    
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT discord_id, nick_mc, discord_name, 
                   tier_por_modalidad, puntos_por_modalidad, 
                   puntos_totales, es_premium
            FROM jugadores
            ORDER BY puntos_totales DESC
        """)
        
        rows = cur.fetchall()
        players_list = []
        
        for row in rows:
            did, nick, dname, tiers_json, puntos_json, ptotal, premium = row
            
            # Crear modalidades
            mods = {}
            if tiers_json:
                for m, t in tiers_json.items():
                    p = 0
                    if puntos_json and m in puntos_json:
                        p = puntos_json[m]
                    mods[m] = {'tier': t, 'tier_display': t, 'puntos': p}
            
            # Agregar jugador
            players_list.append({
                'id': did,
                'name': nick or dname,
                'points': ptotal or 0,
                'es_premium': 'si' if premium == 'si' else 'no',
                'modalidades': mods
            })
        
        conn.close()
        
        # CRÍTICO: Retornar con "players"
        return jsonify({
            'mode': mode,
            'players': players_list,
            'total_players': len(players_list)
        })
        
    except Exception as e:
        if conn:
            conn.close()
        print(f"Error: {e}")
        return jsonify({
            'mode': mode,
            'players': [],
            'total_players': 0,
            'error': str(e)
        }), 500

@app.route('/api/player/<discord_id>')
def get_player(discord_id):
    """Player info"""
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database error'}), 500
    
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT discord_id, nick_mc, discord_name, 
                   tier_por_modalidad, puntos_por_modalidad, 
                   puntos_totales, es_premium
            FROM jugadores
            WHERE discord_id = %s
        """, (discord_id,))
        
        row = cur.fetchone()
        if not row:
            conn.close()
            return jsonify({'error': 'Player not found'}), 404
        
        did, nick, dname, tiers_json, puntos_json, ptotal, premium = row
        
        # Calcular posición
        cur.execute("SELECT COUNT(*) + 1 FROM jugadores WHERE puntos_totales > %s", (ptotal,))
        pos = cur.fetchone()[0]
        
        # Tiers
        tiers_dict = {}
        if tiers_json:
            for m, t in tiers_json.items():
                p = puntos_json.get(m, 0) if puntos_json else 0
                tiers_dict[m] = {'tier': t, 'tier_display': t, 'puntos': p}
        
        conn.close()
        
        return jsonify({
            'id': did,
            'name': nick or dname,
            'discord_name': dname,
            'nick_mc': nick,
            'puntos_totales': ptotal or 0,
            'es_premium': 'si' if premium == 'si' else 'no',
            'tiers': tiers_dict,
            'position': pos,
            'tested': True
        })
        
    except Exception as e:
        if conn:
            conn.close()
        return jsonify({'error': str(e)}), 500

@app.route('/api/stats')
def get_stats():
    """Stats"""
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database error'}), 500
    
    try:
        cur = conn.cursor()
        
        cur.execute("SELECT COUNT(*) FROM jugadores")
        total_players = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM resultados")
        total_tests = cur.fetchone()[0]
        
        cur.execute("""
            SELECT tester_name, COUNT(*) as tests
            FROM resultados
            GROUP BY tester_id, tester_name
            ORDER BY tests DESC
            LIMIT 5
        """)
        
        top_testers = [{'name': r[0], 'tests': r[1]} for r in cur.fetchall()]
        
        conn.close()
        
        return jsonify({
            'total_players': total_players,
            'total_tests': total_tests,
            'top_testers': top_testers
        })
        
    except Exception as e:
        if conn:
            conn.close()
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
