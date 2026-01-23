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

def organize_by_tiers(mode='overall'):
    """Obtiene jugadores organizados por tiers desde PostgreSQL"""
    conn = get_db_connection()
    if not conn:
        return {'tiers': {}, 'all_players': []}
    
    try:
        cur = conn.cursor()
        
        # Obtener todos los jugadores
        cur.execute("""
            SELECT discord_id, nick_mc, discord_name, 
                   tier_por_modalidad, puntos_totales, es_premium
            FROM jugadores
            WHERE tier_por_modalidad IS NOT NULL
        """)
        
        rows = cur.fetchall()
        all_players = []
        
        for row in rows:
            discord_id, nick_mc, discord_name, tier_por_modalidad, puntos_totales, es_premium = row
            
            # Determinar tier según modalidad
            tier = None
            if mode == 'overall':
                # Tier general (basado en puntos totales)
                if puntos_totales >= 90:
                    tier = 'HT1'
                elif puntos_totales >= 80:
                    tier = 'LT1'
                elif puntos_totales >= 70:
                    tier = 'HT2'
                elif puntos_totales >= 60:
                    tier = 'LT2'
                elif puntos_totales >= 50:
                    tier = 'HT3'
                elif puntos_totales >= 40:
                    tier = 'LT3'
                elif puntos_totales >= 30:
                    tier = 'HT4'
                elif puntos_totales >= 20:
                    tier = 'LT4'
                elif puntos_totales >= 10:
                    tier = 'HT5'
                else:
                    tier = 'LT5'
            else:
                # Tier por modalidad específica
                if tier_por_modalidad and mode in tier_por_modalidad:
                    tier = tier_por_modalidad[mode]
            
            if tier:
                all_players.append({
                    'discord_id': discord_id,
                    'nick_mc': nick_mc,
                    'discord_name': discord_name,
                    'tier': tier,
                    'puntos_totales': puntos_totales,
                    'es_premium': es_premium == 'si'
                })
        
        # Organizar por tiers
        tiers = {}
        for player in all_players:
            tier = player['tier']
            if tier not in tiers:
                tiers[tier] = []
            tiers[tier].append(player)
        
        return {
            'tiers': tiers,
            'all_players': all_players
        }
        
    except Exception as e:
        print(f"❌ Error obteniendo jugadores: {e}")
        return {'tiers': {}, 'all_players': []}
    finally:
        conn.close()

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
    """Obtiene rankings por modalidad"""
    valid_modes = ['overall', 'Mace', 'Sword', 'UHC', 'Crystal', 'NethOP', 'SMP', 'Axe', 'Dpot']
    
    if mode not in valid_modes:
        return jsonify({
            'error': 'Invalid mode',
            'valid_modes': valid_modes
        }), 400
    
    data = organize_by_tiers(mode)
    
    return jsonify({
        'mode': mode,
        'tiers': data['tiers'],
        'total_players': len(data['all_players'])
    })

@app.route('/api/player/<discord_id>')
def get_player(discord_id):
    """Obtiene información de un jugador"""
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    
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
            return jsonify({'error': 'Player not found'}), 404
        
        return jsonify({
            'discord_id': row[0],
            'nick_mc': row[1],
            'discord_name': row[2],
            'tier_por_modalidad': row[3],
            'puntos_por_modalidad': row[4],
            'puntos_totales': row[5],
            'es_premium': row[6] == 'si',
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
