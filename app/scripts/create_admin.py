import argparse
import getpass

from sqlalchemy.orm import Session

from app.core.security import hash_password
from app.db.base import Base
from app.db.session import SessionLocal, engine
from app.models.user import User


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Cria o primeiro usuario administrador no banco local.",
    )
    parser.add_argument("--username", default="Administrador", help="Nome de usuario")
    parser.add_argument("--email", default="admin@local", help="Email do administrador")
    parser.add_argument(
        "--password",
        default=None,
        help="Senha inicial. Se omitido, sera solicitada no terminal.",
    )
    return parser.parse_args()


def _read_password_from_prompt() -> str:
    password = getpass.getpass("Senha inicial do admin: ")
    confirm = getpass.getpass("Confirme a senha: ")
    if password != confirm:
        raise ValueError("As senhas nao conferem.")
    return password


def _create_admin(session: Session, username: str, email: str, password: str) -> None:
    existing_user = session.query(User).filter(User.email == email).first()
    if existing_user is not None:
        raise ValueError(f"Ja existe usuario com o email: {email}")

    user = User(
        username=username,
        email=email,
        password=hash_password(password),
        admin=True,
    )
    session.add(user)
    session.commit()


def main() -> int:
    args = _parse_args()
    password = args.password or _read_password_from_prompt()

    if len(password) < 6:
        print("Erro: senha deve ter pelo menos 6 caracteres.")
        return 1

    Base.metadata.create_all(bind=engine)

    session = SessionLocal()
    try:
        _create_admin(
            session=session,
            username=args.username,
            email=args.email,
            password=password,
        )
    except ValueError as exc:
        print(f"Erro: {exc}")
        return 1
    finally:
        session.close()

    print("Admin criado com sucesso.")
    print(f"Email: {args.email}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
