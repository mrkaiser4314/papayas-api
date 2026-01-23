"""
API para Papayas Tierlist - Con PostgreSQL
Lee datos directamente de la base de datos PostgreSQL
"""

from flask import Flask, jsonify, request
from flask_cors import CORS
import os
import psycopg2
from datetime import datetime

app = Flask(__name__)
CORS(app)

def get_db_connection():
    """Obtiene conexión a PostgreSQL"""
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        print("❌ No se encontró DATABASE_URL")
        return None
    
    try:
        conn = psycopg2.connect(database_url)
        return conn
    except Exception as e:
        print(f"❌ Error conectando a PostgreSQL: {e}")
        return None

@app.route('/')
def home():
    return jsonify({
        'status': 'online',
        'message': 'Papayas Tierlist API with PostgreSQL',
        'endpoints': {
            '/api/rankings/<mode>': 'Get player rankings by mode',
            '/api/player/<discord_id>': 'Get player info',
            '/api/stats': 'Get general statistics',
            '/health': 'Health check'
        }
    })

@app.route('/health')
def health():
    """Endpoint de salud"""
    conn = get_db_connection()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM resultados")
            count = cur.fetchone()[0]
            conn.close()
            
            return jsonify({
                'status': 'ok',
                'database': 'connected',
                'total_tests': count
            })
        except:
            conn.close()
            return jsonify({
                'status': 'error',
                'database': 'error'
            }), 500
    else:
        return jsonify({
            'status': 'error',
            'database': 'disconnected'
        }), 500

@app.route('/api/rankings/<mode>')
def get_rankings(mode):
    """Obtiene rankings por modalidad - Compatible con frontend"""
    valid_modes = ['overall', 'Mace', 'Sword', 'UHC', 'Crystal', 'NethOP', 'SMP', 'Axe', 'Dpot']
    
    if mode not in valid_modes:
        return jsonify({
            'error': 'Invalid mode',
            'valid_modes': valid_modes
        }), 400
    
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
        
        # Obtener todos los jugadores con sus datos
        cur.execute("""
            SELECT discord_id, nick_mc, discord_name, 
                   tier_por_modalidad, puntos_por_modalidad, 
                   puntos_totales, es_premium
            FROM jugadores
            ORDER BY puntos_totales DESC
        """)
        
        rows = cur.fetchall()
        players_array = []
        tiers_dict = {}
        
        for row in rows:
            discord_id, nick_mc, discord_name, tier_por_modalidad, puntos_por_modalidad, puntos_totales, es_premium = row
            
            # Determinar tier para el modo actual
            tier_actual = None
            if mode == 'overall':
                # Tier general basado en puntos totales
                if puntos_totales >= 90:
                    tier_actual = 'HT1'
                elif puntos_totales >= 80:
                    tier_actual = 'LT1'
                elif puntos_totales >= 70:
                    tier_actual = 'HT2'
                elif puntos_totales >= 60:
                    tier_actual = 'LT2'
                elif puntos_totales >= 50:
                    tier_actual = 'HT3'
                elif puntos_totales >= 40:
                    tier_actual = 'LT3'
                elif puntos_totales >= 30:
                    tier_actual = 'HT4'
                elif puntos_totales >= 20:
                    tier_actual = 'LT4'
                elif puntos_totales >= 10:
                    tier_actual = 'HT5'
                else:
                    tier_actual = 'LT5'
            else:
                # Tier específico de modalidad
                if tier_por_modalidad and mode in tier_por_modalidad:
                    tier_actual = tier_por_modalidad[mode]
            
            # Formatear modalidades para el frontend
            modalidades = {}
            if tier_por_modalidad:
                for modo, tier in tier_por_modalidad.items():
                    puntos = 0
                    if puntos_por_modalidad and modo in puntos_por_modalidad:
                        puntos = puntos_por_modalidad[modo]
                    
                    modalidades[modo] = {
                        'tier': tier,
                        'tier_display': tier,
                        'puntos': puntos
                    }
            
            # Crear objeto jugador
            player_obj = {
                'id': discord_id,
                'name': nick_mc or discord_name,
                'points': puntos_totales or 0,
                'es_premium': 'si' if es_premium == 'si' else 'no',
                'modalidades': modalidades,
                'tier': tier_actual
            }
            
            players_array.append(player_obj)
            
            # Agregar a tiers_dict
            if tier_actual:
                if tier_actual not in tiers_dict:
                    tiers_dict[tier_actual] = []
                tiers_dict[tier_actual].append(player_obj)
        
        return jsonify({
            'mode': mode,
            'players': players_array,
            'tiers': tiers_dict,
            'total_players': len(players_array)
        })
        
    except Exception as e:
        print(f"❌ Error obteniendo rankings: {e}")
        return jsonify({
            'mode': mode,
            'players': [],
            'tiers': {},
            'total_players': 0,
            'error': str(e)
        }), 500
    finally:
        conn.close()

@app.route('/api/player/<discord_id>')
def get_player(discord_id):
    """Obtiene información detallada de un jugador"""
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    
    try:
        cur = conn.cursor()
        
        # Obtener datos del jugador
        cur.execute("""
            SELECT discord_id, nick_mc, discord_name, 
                   tier_por_modalidad, puntos_por_modalidad, 
                   puntos_totales, es_premium
            FROM jugadores
            WHERE discord_id = %s
        """, (discord_id,))
        
        row = cur.fetchone()
        
        if not row:
            return jsonify({'error': 'Player not found'}), 404
        
        discord_id, nick_mc, discord_name, tier_por_modalidad, puntos_por_modalidad, puntos_totales, es_premium = row
        
        # Calcular posición
        cur.execute("""
            SELECT COUNT(*) + 1
            FROM jugadores
            WHERE puntos_totales > %s
        """, (puntos_totales,))
        position = cur.fetchone()[0]
        
        # Formatear tiers
        tiers = {}
        if tier_por_modalidad:
            for modo, tier in tier_por_modalidad.items():
                puntos = 0
                if puntos_por_modalidad and modo in puntos_por_modalidad:
                    puntos = puntos_por_modalidad[modo]
                
                tiers[modo] = {
                    'tier': tier,
                    'tier_display': tier,
                    'puntos': puntos
                }
        
        return jsonify({
            'id': discord_id,
            'name': nick_mc or discord_name,
            'discord_name': discord_name,
            'nick_mc': nick_mc,
            'puntos_totales': puntos_totales or 0,
            'es_premium': 'si' if es_premium == 'si' else 'no',
            'tiers': tiers,
            'position': position,
            'tested': True
        })
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

@app.route('/api/stats')
def get_stats():
    """Obtiene estadísticas generales"""
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    
    try:
        cur = conn.cursor()
        
        # Total de jugadores
        cur.execute("SELECT COUNT(*) FROM jugadores")
        total_players = cur.fetchone()[0]
        
        # Total de tests
        cur.execute("SELECT COUNT(*) FROM resultados")
        total_tests = cur.fetchone()[0]
        
        # Top testers
        cur.execute("""
            SELECT tester_name, COUNT(*) as tests
            FROM resultados
            GROUP BY tester_id, tester_name
            ORDER BY tests DESC
            LIMIT 5
        """)
        
        top_testers = []
        for row in cur.fetchall():
            top_testers.append({
                'name': row[0],
                'tests': row[1]
            })
        
        return jsonify({
            'total_players': total_players,
            'total_tests': total_tests,
            'top_testers': top_testers
        })
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
