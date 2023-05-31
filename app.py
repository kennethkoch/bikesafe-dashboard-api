from app import app
import os
host='0.0.0.0'
port = int(os.environ.get('PORT', 5000))
if __name__ == "__main__":
    app.run(host='localhost', port=port, debug=True)
